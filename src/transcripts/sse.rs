//! A parser for the transcribe plugin's server-sent event stream.

/// One event from the stream.
#[derive(Debug, PartialEq, Clone)]
pub struct Frame {
    /// The `id:` field, when the frame had one.
    pub id: Option<String>,
    /// The `event:` field, when the frame had one.
    pub event: Option<String>,
    /// The `data:` lines joined with newlines; empty for a ping.
    pub data: String,
}

/// Turns chunks of a streamed body, split anywhere, into frames.
#[derive(Default)]
pub struct SseParser {
    /// Bytes of a line that has not ended yet.
    buf: Vec<u8>,
    id: Option<String>,
    event: Option<String>,
    /// Data lines so far; `None` until the frame has a `data:` line.
    data: Option<String>,
}

impl SseParser {
    /// Takes the next chunk and returns the frames it completes. A partial line is
    /// kept for the next call; a frame with no `data:` line is dropped.
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<Frame> {
        self.buf.extend_from_slice(bytes);
        let mut frames = Vec::new();
        while let Some(end) = self.buf.iter().position(|&b| b == b'\n') {
            let rest = self.buf.split_off(end + 1);
            let mut line = std::mem::replace(&mut self.buf, rest);
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if let Some(frame) = self.line(&String::from_utf8_lossy(&line)) {
                frames.push(frame);
            }
        }
        frames
    }

    fn line(&mut self, line: &str) -> Option<Frame> {
        if line.is_empty() {
            let (id, event, data) = (self.id.take(), self.event.take(), self.data.take());
            return data.map(|data| Frame { id, event, data });
        }
        if line.starts_with(':') {
            return None;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "id" => self.id = Some(value.to_string()),
            "event" => self.event = Some(value.to_string()),
            "data" => match &mut self.data {
                Some(d) => {
                    d.push('\n');
                    d.push_str(value);
                }
                None => self.data = Some(value.to_string()),
            },
            _ => {}
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(event: &str, data: &str) -> Frame {
        Frame { id: None, event: Some(event.into()), data: data.into() }
    }

    #[test]
    fn a_frame_split_across_chunks_is_one_frame() {
        let mut p = SseParser::default();
        assert!(p.feed(b"id: 7\nevent: trans").is_empty());
        assert!(p.feed(b"cript\ndata: {\"a\":").is_empty());
        let got = p.feed(b"1}\n\nevent: x");
        assert_eq!(got, vec![Frame { id: Some("7".into()), event: Some("transcript".into()), data: "{\"a\":1}".into() }]);
        assert!(p.feed(b"\ndata: y\n").is_empty());
        assert_eq!(p.feed(b"\n"), vec![frame("x", "y")]);
    }

    #[test]
    fn multi_line_data_joins_with_newlines() {
        let mut p = SseParser::default();
        let got = p.feed(b"data: one\ndata:two\ndata:  three\n\n");
        assert_eq!(got, vec![Frame { id: None, event: None, data: "one\ntwo\n three".into() }]);
    }

    #[test]
    fn pings_and_comments_are_separate_frames_or_ignored() {
        let mut p = SseParser::default();
        assert_eq!(p.feed(b"event: ping\ndata: \n\n"), vec![frame("ping", "")]);
        assert!(p.feed(b": keepalive\n\n").is_empty());
        assert_eq!(p.feed(b": hi\nevent: ping\ndata:\n\n"), vec![frame("ping", "")]);
    }

    #[test]
    fn crlf_line_endings_work() {
        let mut p = SseParser::default();
        assert!(p.feed(b"event: transcript\r").is_empty());
        let got = p.feed(b"\ndata: {}\r\n\r\n");
        assert_eq!(got, vec![frame("transcript", "{}")]);
    }

    #[test]
    fn a_frame_without_data_is_not_emitted() {
        let mut p = SseParser::default();
        assert!(p.feed(b"\n\nid: 3\nevent: transcript\n\n").is_empty());
        assert_eq!(p.feed(b"data: ok\n\n"), vec![Frame { id: None, event: None, data: "ok".into() }]);
    }
}
