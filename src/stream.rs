use std::marker::Unpin;

use futures::{Future, Stream as FuturesStream, StreamExt};

pub trait Stream {
    type Item<'a>
    where
        Self: 'a;

    fn next(&mut self) -> impl Future<Output = Option<Self::Item<'_>>> + '_;
}

pub struct WrappedStream<S>(S);

impl<S> Stream for WrappedStream<S>
where
    S: FuturesStream + Unpin,
{
    type Item<'a> = S::Item
    where
        Self: 'a;

    async fn next(&mut self) -> Option<Self::Item<'_>> {
        self.0.next().await
    }
}

pub struct Once<T>(Option<T>);

impl<T> Once<T> {
    pub fn new(value: T) -> Self {
        Self(Some(value))
    }
}

pub fn once<T>(value: T) -> Once<T> {
    Once::new(value)
}

impl<T> Stream for Once<T> {
    type Item<'a> = T
    where
        Self: 'a;

    async fn next(&mut self) -> Option<Self::Item<'_>> {
        self.0.take()
    }
}

pub struct Chain<S1, S2>(S1, S2);

impl<S1, S2> Stream for Chain<S1, S2>
where
    S1: Stream,
    for<'x> S2: Stream<Item<'x> = S1::Item<'x>> + 'x,
{
    type Item<'a> = S1::Item<'a>
    where
        Self: 'a;

    async fn next(&mut self) -> Option<Self::Item<'_>> {
        match self.0.next().await {
            Some(x) => Some(x),
            None => self.1.next().await,
        }
    }
}

pub fn chain<S1, S2>(stream1: S1, stream2: S2) -> Chain<S1, S2> {
    Chain(stream1, stream2)
}

pub struct IterChain<I, S, F> {
    iter: I,
    current_stream: S,
    proceed: bool,

    delim_fn: F,
}

impl<I, S, F> IterChain<I, S, F>
where
    S: Default,
{
    pub fn new(iter: I, delim_fn: F) -> Self {
        Self {
            iter,
            current_stream: Default::default(),
            proceed: true,
            delim_fn,
        }
    }
}

impl<I, S, F, G> Stream for IterChain<I, S, F>
where
    I: Iterator<Item = S>,
    S: Stream,
    for<'x> F: FnOnce() -> G + Copy,
    for<'x> G: Into<S::Item<'x>> + 'x,
{
    type Item<'a> = S::Item<'a>
    where
        Self: 'a;

    async fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.proceed {
            self.current_stream = self.iter.next()?;
            self.proceed = false;
        }

        let x = self.current_stream.next().await;

        if x.is_none() {
            self.proceed = true;
            Some((self.delim_fn)().into())
        } else {
            x
        }
    }
}

pub fn iter_chain<I, S, F>(iter: I, delim_fn: F) -> IterChain<I, S, F>
where
    S: Default,
{
    IterChain::new(iter, delim_fn)
}

pub struct Map<S, F> {
    stream: S,
    map_fn: F,
}

impl<S, F> Map<S, F> {
    pub fn new(stream: S, map_fn: F) -> Self {
        Self { stream, map_fn }
    }
}

impl<S, F, B> Stream for Map<S, F>
where
    S: Stream,
    for<'x> F: FnMut(S::Item<'x>) -> B,
{
    type Item<'a> = B
    where
        Self: 'a;

    #[allow(clippy::manual_async_fn)]
    fn next(&mut self) -> impl Future<Output = Option<Self::Item<'_>>> + '_ {
        async {
            match self.stream.next().await {
                Some(value) => Some((self.map_fn)(value)),
                None => None,
            }
        }
    }
}

pub fn map<S, F, B>(stream: S, map_fn: F) -> Map<S, F>
where
    S: Stream,
    for<'x> F: FnMut(S::Item<'x>) -> B,
{
    Map::new(stream, map_fn)
}

pub struct Latch<S> {
    stream: S,
    latch_condition: bool,
}

impl<S> Latch<S> {
    pub fn new(stream: S, latch_condition: bool) -> Self {
        Self {
            stream,
            latch_condition,
        }
    }
}

impl<S> Stream for Latch<S>
where
    S: Stream,
{
    type Item<'a> = S::Item<'a>
    where
        Self: 'a;

    async fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.latch_condition {
            self.stream.next().await
        } else {
            None
        }
    }
}
