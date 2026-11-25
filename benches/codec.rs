//! Codec micro-benchmarks: encode/decode throughput for small and large frames.

use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kyu::codec::{Item, StompCodec};
use kyu::frame::{Command, Frame};
use std::hint::black_box;
use tokio_util::codec::{Decoder, Encoder};

fn message(size: usize) -> Frame {
    Frame::new(Command::Message)
        .with_header("subscription", "sub-0")
        .with_header("message-id", "msg-123456")
        .with_header("destination", "/queue/bench")
        .with_header("content-type", "application/octet-stream")
        .with_body(vec![b'x'; size])
}

fn encoded(frame: &Frame) -> Vec<u8> {
    let mut buf = BytesMut::new();
    StompCodec::new().encode(frame.clone(), &mut buf).unwrap();
    buf.to_vec()
}

fn bench(c: &mut Criterion) {
    let sizes = [0usize, 128, 4 * 1024, 64 * 1024, 1024 * 1024];

    let mut g = c.benchmark_group("encode");
    for &size in &sizes {
        let frame = message(size);
        g.throughput(Throughput::Bytes(encoded(&frame).len() as u64));
        g.bench_with_input(BenchmarkId::from_parameter(size), &frame, |b, f| {
            let mut codec = StompCodec::new();
            let mut buf = BytesMut::with_capacity(size + 256);
            b.iter(|| {
                buf.clear();
                codec.encode(black_box(f.clone()), &mut buf).unwrap();
            });
        });
    }
    g.finish();

    let mut g = c.benchmark_group("decode");
    for &size in &sizes {
        let bytes = encoded(&message(size));
        g.throughput(Throughput::Bytes(bytes.len() as u64));
        g.bench_with_input(BenchmarkId::from_parameter(size), &bytes, |b, bytes| {
            let mut codec = StompCodec::new();
            b.iter(|| {
                let mut buf = BytesMut::from(&bytes[..]);
                black_box(codec.decode(&mut buf).unwrap().unwrap());
            });
        });
    }
    g.finish();

    // Without content-length the decoder must scan for the NULL terminator.
    let mut g = c.benchmark_group("decode_no_content_length");
    for &size in &sizes {
        let mut bytes = b"SEND\ndestination:/queue/bench\n\n".to_vec();
        bytes.extend(std::iter::repeat_n(b'x', size));
        bytes.push(0);
        g.throughput(Throughput::Bytes(bytes.len() as u64));
        g.bench_with_input(BenchmarkId::from_parameter(size), &bytes, |b, bytes| {
            let mut codec = StompCodec::new();
            b.iter(|| {
                let mut buf = BytesMut::from(&bytes[..]);
                black_box(codec.decode(&mut buf).unwrap().unwrap());
            });
        });
    }
    g.finish();

    // A large frame arriving in 16KB TCP-sized chunks: exposes rescanning cost.
    let mut g = c.benchmark_group("decode_chunked_16k");
    for &size in &[64 * 1024usize, 1024 * 1024] {
        let bytes = encoded(&message(size));
        g.throughput(Throughput::Bytes(bytes.len() as u64));
        g.bench_with_input(BenchmarkId::from_parameter(size), &bytes, |b, bytes| {
            let mut codec = StompCodec::new();
            b.iter(|| {
                let mut buf = BytesMut::new();
                for chunk in bytes.chunks(16 * 1024) {
                    buf.extend_from_slice(chunk);
                    if let Some(Item::Frame(f)) = codec.decode(&mut buf).unwrap() {
                        black_box(f);
                    }
                }
            });
        });
    }
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
