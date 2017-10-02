use errors::*;
use std::net::SocketAddr;
use std::sync::Arc;

use {PARSE_ERRORS, INGRESS, EGRESS};

use metric::{Metric, MetricValue};
use parser::parse_metrics;
use num::ToPrimitive;
use smallvec::SmallVec;
use tokio_core::net::UdpCodec;
use tokio_io::codec::{Encoder, Decoder};
use bytes::{BytesMut, BufMut};

use std::sync::atomic::Ordering;

pub struct StatsdCodec;

impl UdpCodec for StatsdCodec {
    type In = Option<SmallVec<[(String, Metric); 16]>>;
    type Out = ();

    fn decode(&mut self, _src: &SocketAddr, buf: &[u8]) -> ::std::io::Result<Self::In> {
        match parse_metrics(buf) {
            Ok(v) => {
                INGRESS.fetch_add(1, Ordering::Relaxed);
                // TODO: log non-zero input len
                Ok(Some(v))
            }
            // TODO log parsing error
            Err(_) => {
                PARSE_ERRORS.fetch_add(1, Ordering::Relaxed);
                Ok(None)
            }
        }
    }

    fn encode(&mut self, _addr: Self::Out, _buf: &mut Vec<u8>) -> SocketAddr {
        unreachable!()
    }
}

// 3rd field is an already converted timestamp - unix time in seconds
pub type OutMetric = Arc<(String, SmallVec<[(&'static str, MetricValue); 32]>, String)>;

pub struct CarbonCodec;

impl Decoder for CarbonCodec {
    type Item = ();
    type Error = Error;

    fn decode(&mut self, _buf: &mut BytesMut) -> Result<Option<Self::Item>> {
        Err(ErrorKind::Unreachable.into())
    }
}

impl Encoder for CarbonCodec {
    type Item = OutMetric; // Metric name, suffix value and timestamp
    type Error = Error;

    fn encode(&mut self, m: Self::Item, buf: &mut BytesMut) -> Result<()> {
        for &(ref prefix, ref value) in m.1.iter() {
            buf.put(&*m.0);
            buf.put(prefix);
            buf.put(" ");
            let formatted = match (value.numer().to_f64(), value.denom().to_f64()) {
                (Some(numer), Some(denom)) => (numer / denom).to_string(),
                _ => return Err(ErrorKind::Format.into()),
            };
            buf.put(formatted + " ");
            buf.put(&m.2);
            buf.put("\n");

            EGRESS.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}
