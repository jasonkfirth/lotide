pub trait TupleAdd<T, A> {
    fn tuple_add(prev: T, add: A) -> Self;
}

impl<A> TupleAdd<(), A> for (A,) {
    fn tuple_add(_prev: (), add: A) -> Self {
        (add,)
    }
}

impl<A, B> TupleAdd<(A,), B> for (A, B) {
    fn tuple_add(prev: (A,), add: B) -> Self {
        (prev.0, add)
    }
}

impl<A, B, C> TupleAdd<(A, B), C> for (A, B, C) {
    fn tuple_add(prev: (A, B), add: C) -> Self {
        (prev.0, prev.1, add)
    }
}

impl<A, B, C, D> TupleAdd<(A, B, C), D> for (A, B, C, D) {
    fn tuple_add(prev: (A, B, C), add: D) -> Self {
        (prev.0, prev.1, prev.2, add)
    }
}
