// AudioWorklet processor with jitter buffer for live radio audio.
// Receives PCM int16 samples via port.postMessage, outputs at AudioContext sample rate.
// Reports buffer health stats back to main thread via 'stats' messages.

class RadioAudioProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.buffer = new Float32Array(32768); // ~4s ring buffer at 8kHz
    this.writePos = 0;
    this.readPos = 0;
    this.buffered = 0;
    this.inputSampleRate = 8000;
    this.active = true;
    this.resampleAccum = 0; // fractional sample accumulator for resampling
    this.playing = false; // playout state: false = buffering, true = playing
    this.silentFrames = 0; // consecutive process() calls with no data

    // Buffer health tracking
    this.underrunCount = 0;      // times buffer emptied while source was actively sending
    this.overflowCount = 0;      // total overflow trims
    this.playoutStartCount = 0;  // how many times playout started
    this.playoutStopCount = 0;   // how many times playout stopped (sustained silence)
    this.minBuffered = Infinity; // min buffer level during playout (samples)
    this.maxBuffered = 0;        // max buffer level during playout (samples)
    this.sumBuffered = 0;        // running sum for average
    this.sampleCount = 0;        // number of process() calls during playout
    this.statsInterval = 0;      // counter for periodic stats reporting
    this.lastEnqueueTime = 0;    // timestamp of last enqueue
    this.enqueueSamplesTotal = 0; // total samples enqueued
    this.prevHadData = false;    // whether previous process() frame had data

    this.port.onmessage = (e) => {
      if (e.data.type === 'audio') {
        this.enqueueSamples(e.data.samples, e.data.sampleRate);
      } else if (e.data.type === 'stop') {
        this.active = false;
      } else if (e.data.type === 'get_stats') {
        this._sendStats();
      }
    };
  }

  enqueueSamples(int16Array, sr) {
    if (sr && sr !== this.inputSampleRate) {
      this.inputSampleRate = sr;
    }

    for (let i = 0; i < int16Array.length; i++) {
      this.buffer[this.writePos] = int16Array[i] / 32768.0;
      this.writePos = (this.writePos + 1) % this.buffer.length;
      this.buffered = Math.min(this.buffered + 1, this.buffer.length);
    }

    this.enqueueSamplesTotal += int16Array.length;
    this.lastEnqueueTime = currentTime;

    // Overflow protection: only trim when buffer exceeds 1.5s (excessive latency)
    // Normal operation: buffer hovers at 0-400ms during active calls
    const maxSamples = Math.floor(this.inputSampleRate * 1.5);
    const targetSamples = Math.floor(this.inputSampleRate * 0.5);
    if (this.buffered > maxSamples) {
      const skip = this.buffered - targetSamples;
      this.readPos = (this.readPos + skip) % this.buffer.length;
      this.buffered -= skip;
      this.overflowCount++;
    }
  }

  process(inputs, outputs, parameters) {
    if (!this.active) return false;

    const output = outputs[0][0]; // mono
    if (!output) return true;

    // Jitter buffer with soft underrun handling:
    // - Accumulate 200ms before first playout (absorb initial jitter)
    // - During playback, ride through brief underruns with silence
    // - Only fully stop after 500ms of sustained empty buffer (~190 process() calls)
    if (!this.playing) {
      const startThreshold = Math.floor(this.inputSampleRate * 0.4); // 400ms
      if (this.buffered >= startThreshold) {
        this.playing = true;
        this.silentFrames = 0;
        this.resampleAccum = 0; // fresh start
        this.playoutStartCount++;
      } else {
        // Still buffering — output silence
        for (let i = 0; i < output.length; i++) {
          output[i] = 0;
        }
        return true;
      }
    }

    // Track buffer level during playout
    if (this.buffered < this.minBuffered) this.minBuffered = this.buffered;
    if (this.buffered > this.maxBuffered) this.maxBuffered = this.buffered;
    this.sumBuffered += this.buffered;
    this.sampleCount++;

    // ratio < 1 when upsampling (e.g. 8000/48000 = 0.167)
    // ratio > 1 when downsampling
    const ratio = this.inputSampleRate / sampleRate;
    let hadData = false;

    for (let i = 0; i < output.length; i++) {
      if (this.buffered > 0) {
        hadData = true;
        // Linear interpolation between input samples
        const idx0 = this.readPos;
        const idx1 = (this.readPos + 1) % this.buffer.length;
        const frac = this.resampleAccum;
        output[i] = this.buffer[idx0] * (1 - frac) + (this.buffered > 1 ? this.buffer[idx1] : this.buffer[idx0]) * frac;

        // Advance fractional position by ratio
        this.resampleAccum += ratio;

        // Consume whole input samples
        while (this.resampleAccum >= 1.0 && this.buffered > 0) {
          this.resampleAccum -= 1.0;
          this.readPos = (this.readPos + 1) % this.buffer.length;
          this.buffered--;
        }
      } else {
        output[i] = 0; // brief underrun — output silence but keep playing
      }
    }

    // Track sustained silence: only stop playout after ~500ms of empty buffer.
    // Radio audio has natural pauses — stopping too early causes repeated
    // stop/start cycles with audible gaps.
    if (!hadData) {
      // Count as underrun only on the transition from data→empty AND only if
      // data was enqueued recently (within ~300ms). This distinguishes real
      // mid-stream buffer starvation from normal end-of-transmission drain.
      if (this.prevHadData && (currentTime - this.lastEnqueueTime) < 0.3) {
        this.underrunCount++;
      }
      this.silentFrames++;
      // 500ms at 48kHz with 128-sample blocks = ~188 frames
      if (this.silentFrames > 188) {
        this.playing = false;
        this.silentFrames = 0;
        this.playoutStopCount++;
      }
    } else {
      this.silentFrames = 0;
    }
    this.prevHadData = hadData;

    // Report stats every ~1s (48000/128 ≈ 375 process() calls per second)
    this.statsInterval++;
    if (this.statsInterval >= 375) {
      this.statsInterval = 0;
      this._sendStats();
    }

    return true;
  }

  _sendStats() {
    const avgBuffered = this.sampleCount > 0 ? this.sumBuffered / this.sampleCount : 0;
    const sr = this.inputSampleRate || 8000;
    this.port.postMessage({
      type: 'stats',
      playing: this.playing,
      buffered: this.buffered,
      bufferedMs: Math.round(this.buffered / sr * 1000),
      avgBufferedMs: Math.round(avgBuffered / sr * 1000),
      minBufferedMs: this.minBuffered === Infinity ? 0 : Math.round(this.minBuffered / sr * 1000),
      maxBufferedMs: Math.round(this.maxBuffered / sr * 1000),
      underruns: this.underrunCount,
      overflows: this.overflowCount,
      playoutStarts: this.playoutStartCount,
      playoutStops: this.playoutStopCount,
      enqueuedSamples: this.enqueueSamplesTotal,
      inputSampleRate: sr,
    });
  }
}

registerProcessor('radio-audio-processor', RadioAudioProcessor);
