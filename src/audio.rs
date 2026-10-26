//! Live audio: the recorder's PCM chunks, fanned out to WebSocket clients and
//! framed as tr-engine frames them.

use std::sync::Arc;

use tokio::sync::broadcast;

/// Frames the bus keeps for a client that has not read them yet.
pub const BUS_CAPACITY: usize = 256;

/// A slice of a call's audio, for the store's system.
#[derive(Clone, Debug, PartialEq)]
pub struct AudioFrame {
    pub system_id: i64,
    pub tgid: u32,
    pub sample_rate: u16,
    /// 16-bit samples, little-endian, as the recorder sent them.
    pub pcm: Arc<[u8]>,
}

/// One publisher (the plugin), any number of clients. A client that falls
/// more than the capacity behind loses the oldest frames; the publisher never waits.
#[derive(Clone, Debug)]
pub struct AudioBus(broadcast::Sender<AudioFrame>);

impl AudioBus {
    pub fn new(capacity: usize) -> AudioBus {
        AudioBus(broadcast::channel(capacity).0)
    }

    /// Never blocks. With no client listening the frame is dropped.
    pub fn publish(&self, f: AudioFrame) {
        let _ = self.0.send(f);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AudioFrame> {
        self.0.subscribe()
    }

    /// Clients listening now.
    pub fn active_streams(&self) -> usize {
        self.0.receiver_count()
    }
}

/// The header length of [`frame_bytes`].
pub const HEADER_LEN: usize = 14;

/// tr-engine's binary frame: `system_id` u16, `tgid` u32, `ts_ms` u32 (since the
/// connection opened), `seq` u16, `sample_rate` u16, all big-endian, then the PCM.
pub fn frame_bytes(f: &AudioFrame, ts_ms: u32, seq: u16) -> Vec<u8> {
    let mut b = Vec::with_capacity(HEADER_LEN + f.pcm.len());
    b.extend_from_slice(&(f.system_id as u16).to_be_bytes());
    b.extend_from_slice(&f.tgid.to_be_bytes());
    b.extend_from_slice(&ts_ms.to_be_bytes());
    b.extend_from_slice(&seq.to_be_bytes());
    b.extend_from_slice(&f.sample_rate.to_be_bytes());
    b.extend_from_slice(&f.pcm);
    b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_frame_header_is_fourteen_big_endian_bytes() {
        let f = AudioFrame { system_id: 3, tgid: 9178, sample_rate: 8000, pcm: Arc::from(&[1u8, 0, 255, 127][..]) };
        let b = frame_bytes(&f, 1234, 7);
        assert_eq!(&b[..14], &[0, 3, 0, 0, 0x23, 0xDA, 0, 0, 0x04, 0xD2, 0, 7, 0x1F, 0x40]);
        assert_eq!(&b[14..], &[1, 0, 255, 127]);
    }

    #[test]
    fn publishing_with_no_client_is_fine_and_a_lagging_client_is_told() {
        let bus = AudioBus::new(2);
        let f = AudioFrame { system_id: 1, tgid: 1, sample_rate: 8000, pcm: Arc::from(&[0u8; 0][..]) };
        bus.publish(f.clone());
        assert_eq!(bus.active_streams(), 0);
        let mut rx = bus.subscribe();
        assert_eq!(bus.active_streams(), 1);
        for _ in 0..3 {
            bus.publish(f.clone());
        }
        assert!(matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Lagged(1))));
        assert_eq!(rx.try_recv().ok(), Some(f));
    }
}
