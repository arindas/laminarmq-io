use futures::{Future, Stream as FuturesStream, StreamExt};
use std::marker::{PhantomData, Unpin};

pub trait Lender {
    type Item<'a>
    where
        Self: 'a;
}

pub trait Stream<L: Lender> {
    fn next<'a>(&'a mut self) -> impl Future<Output = Option<L::Item<'a>>> + 'a
    where
        L: 'a;
}

pub struct OwnedLender<T>(PhantomData<T>);

impl<T> Lender for OwnedLender<T> {
    type Item<'a> = T
    where
        Self: 'a;
}

pub struct WrappedStream<S>(S);

impl<S> Stream<OwnedLender<S::Item>> for WrappedStream<S>
where
    S: FuturesStream + Unpin,
{
    fn next<'a>(
        &'a mut self,
    ) -> impl Future<Output = Option<<OwnedLender<S::Item> as Lender>::Item<'a>>> + 'a
    where
        OwnedLender<S::Item>: 'a,
    {
        self.0.next()
    }
}

pub struct Once<T>(Option<T>);

impl<T> Stream<OwnedLender<T>> for Once<T> {
    async fn next<'a>(&'a mut self) -> Option<<OwnedLender<T> as Lender>::Item<'a>>
    where
        OwnedLender<T>: 'a,
    {
        self.0.take()
    }
}

pub fn once<T>(value: T) -> Once<T> {
    Once(Some(value))
}

pub struct RefOf<T>(PhantomData<T>);

impl<T> Lender for RefOf<T> {
    type Item<'a> = &'a T
    where
        Self: 'a;
}

pub struct OnceRef<'a, T> {
    finished: bool,
    once_ref: &'a T,
}

impl<'a, T> OnceRef<'a, T> {
    pub fn new(once_ref: &'a T) -> Self {
        Self {
            finished: false,
            once_ref,
        }
    }
}

pub fn once_ref<T>(value_ref: &T) -> OnceRef<'_, T> {
    OnceRef::new(value_ref)
}

impl<'x, T> Stream<RefOf<T>> for OnceRef<'x, T> {
    async fn next<'a>(&'a mut self) -> Option<<RefOf<T> as Lender>::Item<'a>>
    where
        RefOf<T>: 'a,
    {
        if self.finished {
            return None;
        }

        self.finished = true;

        Some(self.once_ref)
    }
}

pub struct Chain<S1, S2>(S1, S2);

impl<S1, S2, L> Stream<L> for Chain<S1, S2>
where
    S1: Stream<L>,
    S2: Stream<L>,
    L: Lender,
{
    async fn next<'a>(&'a mut self) -> Option<<L as Lender>::Item<'a>>
    where
        L: 'a,
    {
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

impl<I, S, F, L> Stream<L> for IterChain<I, S, F>
where
    I: Iterator<Item = S>,
    S: Stream<L>,
    L: Lender,
    F: FnOnce(&()) -> L::Item<'_> + Copy,
{
    async fn next<'a>(&'a mut self) -> Option<<L as Lender>::Item<'a>>
    where
        L: 'a,
    {
        if self.proceed {
            self.current_stream = self.iter.next()?;
            self.proceed = false;
        }

        let x = self.current_stream.next().await;

        if x.is_none() {
            self.proceed = true;
            Some((self.delim_fn)(&()))
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

pub struct Map<S, F, AL, BL> {
    stream: S,
    map_fn: F,

    _phantom_data: PhantomData<(AL, BL)>,
}

impl<S, F, AL, BL> Map<S, F, AL, BL> {
    pub fn new(stream: S, map_fn: F) -> Self {
        Self {
            stream,
            map_fn,
            _phantom_data: PhantomData,
        }
    }
}

impl<S, F, AL, BL> Stream<BL> for Map<S, F, AL, BL>
where
    S: Stream<AL>,
    AL: Lender,
    BL: Lender,
    F: for<'x> FnMut(&'x (), AL::Item<'x>) -> BL::Item<'x>,
{
    async fn next<'a>(&'a mut self) -> Option<<BL as Lender>::Item<'a>>
    where
        BL: 'a,
    {
        Some((self.map_fn)(&(), self.stream.next().await?))
    }
}

pub fn map<S, F, AL, BL>(stream: S, map_fn: F) -> Map<S, F, AL, BL>
where
    S: Stream<AL>,
    AL: Lender,
    BL: Lender,
    F: for<'x> FnMut(&'x (), AL::Item<'x>) -> BL::Item<'x>,
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

impl<S, L> Stream<L> for Latch<S>
where
    S: Stream<L>,
    L: Lender,
{
    async fn next<'a>(&'a mut self) -> Option<<L as Lender>::Item<'a>>
    where
        L: 'a,
    {
        if self.latch_condition {
            self.stream.next().await
        } else {
            None
        }
    }
}

pub fn latch<S>(stream: S, latch_condition: bool) -> Latch<S> {
    Latch::new(stream, latch_condition)
}
