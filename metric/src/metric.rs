use std::collections::HashSet;
use std::fmt::Debug;

use bytes::Bytes;
use capnp;
use capnp::message::{Allocator, Builder, HeapAllocator};
use failure::Error;
use failure_derive::Fail;
use serde_derive::{Serialize, Deserialize};

use crate::protocol_capnp::{metric as cmetric, metric_type, gauge};

use num_traits::{Float, AsPrimitive};

#[derive(Fail, Debug)]
pub enum MetricError {
    #[fail(display = "float conversion")]
    FloatToRatio,

    #[fail(display = "bad sampling range")]
    Sampling,

    #[fail(display = "aggregating metrics of different types")]
    Aggregating,

    #[fail(display = "decoding error: {}", _0)]
    Capnp(capnp::Error),

    #[fail(display = "schema error: {}", _0)]
    CapnpSchema(capnp::NotInSchema),
}

// Percentile counter. Not safe. Requires at least two elements in vector
// vector must be sorted
pub fn percentile<F>(vec: &Vec<F>, nth: F) -> F
where
F: Float + AsPrimitive<usize>,
{
    let last = F::from(vec.len() - 1).unwrap(); // usize to float should be ok for both f32 and f64
    if last == F::zero() {
        return vec[0].clone();
    }

    let k = nth * last;
    let f = k.floor();
    let c = k.ceil();

    if c == f {
        // exact nth percentile have been found
        return vec[k.as_()].clone();
    }

    let m0 = c - k;
    let m1 = k - f;
    let d0 = vec[f.as_()].clone() * m0;
    let d1 = vec[c.as_()].clone() * m1;
    let res = d0 + d1;
    res
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetricType<F>
where
F: Copy + PartialEq + Debug,
{
    Counter,
    DiffCounter(F),
    Timer(Vec<F>),
    Gauge(Option<i8>),
    Set(HashSet<u64>),
    //    Histogram,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Metric<F>
where
F: Copy + PartialEq + Debug,
{
    pub value: F,
    pub mtype: MetricType<F>,
    pub timestamp: Option<u64>,
    pub update_counter: u32,
    pub sampling: Option<f32>,
}

pub trait FromF64
{
    fn from_f64(value: f64) -> Self;
}

impl FromF64 for f64
{
    fn from_f64(value: f64) -> Self {
        value
    }
}

impl FromF64 for f32
{
    // TODO specilaization will give us a possibility to use any other float the same way
    fn from_f64(value: f64) -> Self {
        let (mantissa, exponent, sign) = Float::integer_decode(value);
        let sign_f = sign as f32;
        let mantissa_f = mantissa as f32;
        let exponent_f = 2f32.powf(exponent as f32);
        sign_f * mantissa_f * exponent_f
    }
}

impl<F> Metric<F>
where
F: Float + Debug + AsPrimitive<f64> + FromF64 + Sync,
{
    pub fn new(
        value: F,
        mtype: MetricType<F>,
        timestamp: Option<u64>,
        sampling: Option<f32>,
        ) -> Result<Self, MetricError> {
        let mut metric = Metric {
            value,
            mtype,
            timestamp,
            sampling,
            update_counter: 1,
        };

        if let MetricType::Timer(ref mut agg) = metric.mtype {
            agg.push(metric.value)
        };
        if let MetricType::Set(ref mut hs) = metric.mtype {
            hs.insert(metric.value.as_().to_bits());
        };
        Ok(metric)
    }

    pub fn aggregate(&mut self, new: Metric<F>) -> Result<(), Error> {
        use self::MetricType::*;
        self.update_counter += new.update_counter;
        match (&mut self.mtype, new.mtype) {
            (&mut Counter, Counter) => {
                self.value = self.value + new.value;
            }
            (&mut DiffCounter(ref mut previous), DiffCounter(_)) => {
                // FIXME: this is most probably incorrect when joining with another
                // non-fresh metric count2 != 1
                let prev = *previous;
                let diff = if new.value > prev {
                    new.value - prev
                } else {
                    new.value
                };
                *previous = new.value;
                self.value = self.value + diff;
            }
            (&mut Gauge(_), Gauge(Some(1))) => {
                self.value = self.value + new.value;
            }
            (&mut Gauge(_), Gauge(Some(-1))) => {
                self.value = self.value - new.value;
            }
            (&mut Gauge(_), Gauge(None)) => {
                self.value = new.value;
            }
            (&mut Gauge(_), Gauge(Some(_))) => {
                return Err(MetricError::Aggregating.into());
            }
            (&mut Timer(ref mut agg), Timer(ref mut agg2)) => {
                self.value = new.value;
                agg.append(agg2);
            }
            (&mut Set(ref mut hs), Set(ref mut hs2)) => {
                hs.extend(hs2.iter());
            }

            (_m1, _m2) => {
                return Err(MetricError::Aggregating.into());
            }
        };
        Ok(())
    }


    pub fn from_capnp<'a>(
        reader: cmetric::Reader<'a>,
        ) -> Result<(Bytes, Metric<F>), MetricError> {
        let name = reader.get_name().map_err(MetricError::Capnp)?.into();
        let value: F = F::from_f64(reader.get_value());

        let mtype = reader.get_type().map_err(MetricError::Capnp)?;
        let mtype = match mtype.which().map_err(MetricError::CapnpSchema)? {
            metric_type::Which::Counter(()) => MetricType::Counter,
            metric_type::Which::DiffCounter(c) => MetricType::DiffCounter(F::from_f64(c)),
            metric_type::Which::Gauge(reader) => {
                let reader = reader.map_err(MetricError::Capnp)?;
                match reader.which().map_err(MetricError::CapnpSchema)? {
                    gauge::Which::Unsigned(()) => MetricType::Gauge(None),
                    gauge::Which::Signed(sign) => MetricType::Gauge(Some(sign)),
                }
            }
            metric_type::Which::Timer(reader) => {
                let reader = reader.map_err(MetricError::Capnp)?;
                let mut v = Vec::new();
                v.reserve_exact(reader.len() as usize);
                reader.iter().map(|ms| v.push(FromF64::from_f64(ms))).last();
                MetricType::Timer(v)
            }
            metric_type::Which::Set(reader) => {
                let reader = reader.map_err(MetricError::Capnp)?;
                let v = reader.iter().collect();
                MetricType::Set(v)
            }
        };

        let timestamp = if reader.has_timestamp() {
            Some(reader.get_timestamp().map_err(MetricError::Capnp)?.get_ts())
        } else {
            None
        };

        let (sampling, up_counter) = match reader.get_meta() {
            Ok(reader) => (
                if reader.has_sampling() {
                    reader
                        .get_sampling()
                        .ok()
                        .map(|reader| reader.get_sampling())
                } else {
                    None
                },
                Some(reader.get_update_counter()),
                ),
            Err(_) => (None, None),
        };

        // we should NOT use Metric::new here because it is not a newly created metric
        // we'd get duplicate value in timer/set metrics if we used new
        let metric: Metric<F> = Metric {
            value: value,
            mtype,
            timestamp,
            sampling,
            update_counter: if let Some(c) = up_counter { c } else { 1 },
        };

        Ok((name, metric))
    }

    pub fn fill_capnp<'a>(&self, builder: &mut cmetric::Builder<'a>) {
        // no name is known at this stage
        // value
        builder.set_value(self.value.as_());
        // mtype
        {
            let mut t_builder = builder.reborrow().init_type();
            match self.mtype {
                MetricType::Counter => t_builder.set_counter(()),
                MetricType::DiffCounter(v) => t_builder.set_diff_counter(v.as_()),
                MetricType::Gauge(sign) => {
                    let mut g_builder = t_builder.init_gauge();
                    match sign {
                        Some(v) => g_builder.set_signed(v),
                        None => g_builder.set_unsigned(()),
                    }
                }
                MetricType::Timer(ref v) => {
                    let mut timer_builder = t_builder.init_timer(v.len() as u32);
                    v.iter()
                        .enumerate()
                        .map(|(idx, value)| {
                            let value: f64 = (*value).as_();
                            timer_builder.set(idx as u32, value);
                        })
                    .last();
                }
                MetricType::Set(ref v) => {
                    let mut set_builder = t_builder.init_set(v.len() as u32);
                    v.iter()
                        .enumerate()
                        .map(|(idx, value)| {
                            set_builder.set(idx as u32, *value);
                        })
                    .last();
                }
            }
        }

        // timestamp
        {
            if let Some(timestamp) = self.timestamp {
                builder.reborrow().init_timestamp().set_ts(timestamp);
            }
        }

        // meta
        let mut m_builder = builder.reborrow().init_meta();
        if let Some(sampling) = self.sampling {
            m_builder.reborrow().init_sampling().set_sampling(sampling)
        }
        m_builder.set_update_counter(self.update_counter);
    }

    // may be useful in future somehow
    pub fn as_capnp<A: Allocator>(&self, allocator: A) -> Builder<A> {
        let mut builder = Builder::new(allocator);
        {
            let mut root = builder.init_root::<cmetric::Builder>();
            self.fill_capnp(&mut root);
        }
        builder
    }
    // may be useful in future somehow
    pub fn as_capnp_heap(&self) -> Builder<HeapAllocator> {
        let allocator = HeapAllocator::new();
        let mut builder = Builder::new(allocator);
        {
            let mut root = builder.init_root::<cmetric::Builder>();
            self.fill_capnp(&mut root);
        }
        builder
    }
}

impl<F> IntoIterator for Metric<F>
where
F: Float + Debug + FromF64 + AsPrimitive<usize>
{
    type Item = (&'static str, F);
    type IntoIter = MetricIter<F>;
    fn into_iter(self) -> Self::IntoIter {
        MetricIter::new(self)
    }
}

pub struct MetricIter<F>
where
F: Float + Debug + FromF64 + AsPrimitive<usize>
{
    m: Metric<F>,
    count: usize,
    // cached sum to avoid recounting
    timer_sum: Option<F>,
}

impl<F> MetricIter<F>
where
F: Float + Debug + FromF64 + AsPrimitive<usize>
{
    fn new(mut metric: Metric<F>) -> Self {
        let sum = if let MetricType::Timer(ref mut agg) = metric.mtype {
            agg.sort_unstable_by(|ref v1, ref v2| v1.partial_cmp(v2).unwrap());
            let first = agg.first().unwrap();
            Some(agg.iter().skip(1).fold(*first, |acc, &v| acc + v))
        } else {
            None
        };
        MetricIter {
            m: metric,
            count: 0,
            timer_sum: sum,
        }
    }
}

impl<F> Iterator for MetricIter<F>
where
F: Float + Debug + FromF64 + AsPrimitive<usize>
{
    type Item = (&'static str, F);

    fn next(&mut self) -> Option<Self::Item> {
        let res: Option<Self::Item> = match &self.m.mtype {
            &MetricType::Counter if self.count == 0 => Some(("", self.m.value.into())),
            &MetricType::DiffCounter(_) if self.count == 0 => Some(("", self.m.value.into())),
            &MetricType::Gauge(_) if self.count == 0 => Some(("", self.m.value.into())),
            &MetricType::Timer(ref agg) => {
                match self.count {
                    0 => Some((".count", F::from_f64(agg.len() as f64) )),
                    // agg.len() = 0 is impossible here because of metric creation logic.
                    // For additional panic safety and to ensure unwrapping is safe here
                    // this will return None interrupting the iteration and making other
                    // aggregations unreachable since they are useless in that case
                    1 => agg.last().map(|last| (".last", (*last).into())),
                    2 => Some((".min", agg[0])),
                    3 => Some((".max", agg[agg.len() - 1])),
                    4 => Some((".sum", self.timer_sum.unwrap())),
                    5 => Some((".median", percentile(agg, F::from_f64(0.5)))),
                    6 => {
                        let len: F = F::from_f64(agg.len() as f64);
                        Some((".mean", self.timer_sum.unwrap() / len))
                    }
                    7 => Some((".percentile.75", percentile(agg, F::from_f64(0.75)))),
                    8 => Some((".percentile.95", percentile(agg, F::from_f64(0.95)))),
                    9 => Some((".percentile.98", percentile(agg, F::from_f64(0.98)))),
                    10 => Some((".percentile.99", percentile(agg, F::from_f64(0.99)))),
                    11 => Some((".percentile.999", percentile(agg, F::from_f64(0.999)))),
                    _ => None,
                }
            }
            &MetricType::Set(ref hs) if self.count == 0 => Some(("", F::from_f64(hs.len() as f64))),
            _ => None,
        };
        self.count += 1;
        res
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use capnp::serialize::{read_message, write_message};
    type Float = f64;

    fn capnp_test(metric: Metric<Float>) {
        let mut buf = Vec::new();
        write_message(&mut buf, &metric.as_capnp_heap()).unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let reader = read_message(&mut cursor, capnp::message::DEFAULT_READER_OPTIONS).unwrap();
        let reader = reader.get_root().unwrap();
        let (_, rmetric) = Metric::<Float>::from_capnp(reader).unwrap();
        assert_eq!(rmetric, metric);
    }

    #[test]
    fn test_metric_capnp_counter() {
        let mut metric1 = Metric::new(1f64, MetricType::Counter, Some(10), Some(0.1)).unwrap();
        let metric2 = Metric::new(2f64, MetricType::Counter, None, None).unwrap();
        metric1.aggregate(metric2).unwrap();
        capnp_test(metric1);
    }

    #[test]
    fn test_metric_capnp_diffcounter() {
        let mut metric1 =
            Metric::new(1f64, MetricType::DiffCounter(0.1f64), Some(20), Some(0.2)).unwrap();
        let metric2 = Metric::new(1f64, MetricType::DiffCounter(0.5f64), None, None).unwrap();
        metric1.aggregate(metric2).unwrap();
        capnp_test(metric1);
    }

    #[test]
    fn test_metric_capnp_timer() {
        let mut metric1 =
            Metric::new(1f64, MetricType::Timer(Vec::new()), Some(10), Some(0.1)).unwrap();
        let metric2 = Metric::new(2f64, MetricType::Timer(vec![3f64]), None, None).unwrap();
        metric1.aggregate(metric2).unwrap();
        assert!(if let MetricType::Timer(ref v) = metric1.mtype {
            v.len() == 3
        } else {
            false
        });

        capnp_test(metric1);
    }

    #[test]
    fn test_metric_capnp_gauge() {
        let mut metric1 = Metric::new(1f64, MetricType::Gauge(None), Some(10), Some(0.1)).unwrap();
        let metric2 = Metric::new(2f64, MetricType::Gauge(Some(-1)), None, None).unwrap();
        metric1.aggregate(metric2).unwrap();

        capnp_test(metric1);
    }

    #[test]
    fn test_metric_capnp_set() {
        let mut set1 = HashSet::new();
        set1.extend(vec![10u64, 20u64, 10u64].into_iter());
        let mut metric1 = Metric::new(1f64, MetricType::Set(set1), Some(10), Some(0.1)).unwrap();
        let mut set2 = HashSet::new();
        set2.extend(vec![10u64, 30u64].into_iter());
        let metric2 = Metric::new(2f64, MetricType::Set(set2), None, None).unwrap();
        metric1.aggregate(metric2).unwrap();

        capnp_test(metric1);
    }
}
