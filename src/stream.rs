use std::marker::Unpin;

use futures::{Future, Stream as FuturesStream, StreamExt};

pub trait Stream {
    type Item<'a>
    where
        Self: 'a;

    fn next(&mut self) -> impl Future<Output = Option<Self::Item<'_>>>;
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

impl<S1, S2, T> Stream for Chain<S1, S2>
where
    for<'x> S1: Stream<Item<'x> = T> + 'x,
    for<'x> S2: Stream<Item<'x> = T> + 'x,
{
    type Item<'a> = T
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

pub struct IterChain<I, S> {
    iter: I,
    current_stream: S,
}

impl<I, S> IterChain<I, S>
where
    S: Default,
{
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            current_stream: Default::default(),
        }
    }
}

impl<I, S, T> Stream for IterChain<I, S>
where
    I: Iterator<Item = S>,
    for<'x> S: Stream<Item<'x> = T> + 'x,
{
    type Item<'a> = T
    where
        Self: 'a;

    async fn next(&mut self) -> Option<Self::Item<'_>> {
        match self.current_stream.next().await {
            Some(value) => Some(value),
            None => {
                self.current_stream = self.iter.next()?;
                self.current_stream.next().await
            }
        }
    }
}

pub fn iter_chain<I, S>(iter: I) -> IterChain<I, S>
where
    S: Default,
{
    IterChain::new(iter)
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

impl<S, F, A, B> Stream for Map<S, F>
where
    for<'x> S: Stream<Item<'x> = A> + 'x,
    F: FnMut(A) -> B,
{
    type Item<'a> = B
    where
        Self: 'a;

    async fn next(&mut self) -> Option<Self::Item<'_>> {
        match self.stream.next().await {
            Some(value) => Some((self.map_fn)(value)),
            None => None,
        }
    }
}

pub fn map<S, F>(stream: S, map_fn: F) -> Map<S, F> {
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
