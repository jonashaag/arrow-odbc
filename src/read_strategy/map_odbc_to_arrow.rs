use std::{marker::PhantomData, sync::Arc};

use arrow::{
    array::{ArrayRef, PrimitiveBuilder},
    datatypes::ArrowPrimitiveType,
};
use chrono::NaiveDateTime;
use odbc_api::buffers::{AnySlice, BufferDesc, Item};
use thiserror::Error;

use super::ReadStrategy;

pub trait MapOdbcToArrow {
    type ArrowElement;

    fn map_with<U>(
        nullable: bool,
        odbc_to_arrow: impl Fn(&U) -> Result<Self::ArrowElement, MappingError> + 'static + Send + Sync,
    ) -> Box<dyn ReadStrategy>
    where
        U: Send + Sync + Item + 'static;

    fn identical(nullable: bool) -> Box<dyn ReadStrategy>
    where
        Self::ArrowElement: Item;
}

impl<T> MapOdbcToArrow for T
where
    T: Send + Sync + ArrowPrimitiveType,
{
    type ArrowElement = T::Native;

    fn map_with<U>(
        nullable: bool,
        odbc_to_arrow: impl Fn(&U) -> Result<Self::ArrowElement, MappingError> + 'static + Send + Sync,
    ) -> Box<dyn ReadStrategy>
    where
        U: Send + Sync + Item + 'static,
    {
        if nullable {
            Box::new(NullableStrategy::<Self, U, _>::new(odbc_to_arrow))
        } else {
            Box::new(NonNullableStrategy::<Self, U, _>::new(odbc_to_arrow))
        }
    }

    fn identical(nullable: bool) -> Box<dyn ReadStrategy>
    where
        Self::ArrowElement: Item,
    {
        if nullable {
            Box::new(NullableDirectStrategy::<Self>::new())
        } else {
            Box::new(NonNullDirectStrategy::<Self>::new())
        }
    }
}

struct NonNullDirectStrategy<T> {
    phantom: PhantomData<T>,
}

impl<T> NonNullDirectStrategy<T> {
    fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<T> ReadStrategy for NonNullDirectStrategy<T>
where
    T: Send + Sync,
    T: ArrowPrimitiveType,
    T::Native: Item,
{
    fn buffer_desc(&self) -> BufferDesc {
        T::Native::buffer_desc(false)
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let slice = T::Native::as_slice(column_view).unwrap();
        let mut builder = PrimitiveBuilder::<T>::with_capacity(slice.len());
        builder.append_slice(slice);
        Ok(Arc::new(builder.finish()))
    }
}

struct NullableDirectStrategy<T> {
    phantom: PhantomData<T>,
}

impl<T> NullableDirectStrategy<T> {
    fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<T> ReadStrategy for NullableDirectStrategy<T>
where
    T: Send + Sync,
    T: ArrowPrimitiveType,
    T::Native: Item,
{
    fn buffer_desc(&self) -> BufferDesc {
        T::Native::buffer_desc(true)
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let values = T::Native::as_nullable_slice(column_view).unwrap();
        let mut builder = PrimitiveBuilder::<T>::with_capacity(values.len());
        for value in values {
            builder.append_option(value.copied());
        }
        Ok(Arc::new(builder.finish()))
    }
}

struct NonNullableStrategy<P, O, F> {
    _primitive_type: PhantomData<P>,
    _odbc_item: PhantomData<O>,
    odbc_to_arrow: F,
}

impl<P, O, F> NonNullableStrategy<P, O, F> {
    fn new(odbc_to_arrow: F) -> Self {
        Self {
            _primitive_type: PhantomData,
            _odbc_item: PhantomData,
            odbc_to_arrow,
        }
    }
}

impl<P, O, F> ReadStrategy for NonNullableStrategy<P, O, F>
where
    P: Send + Sync + ArrowPrimitiveType,
    O: Send + Sync + Item,
    F: Send + Sync + Fn(&O) -> Result<P::Native, MappingError>,
{
    fn buffer_desc(&self) -> BufferDesc {
        O::buffer_desc(false)
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let slice = column_view.as_slice::<O>().unwrap();
        let mut builder = PrimitiveBuilder::<P>::with_capacity(slice.len());
        for odbc_value in slice {
            builder.append_value((self.odbc_to_arrow)(odbc_value)?);
        }
        Ok(Arc::new(builder.finish()))
    }
}

struct NullableStrategy<P, O, F> {
    _primitive_type: PhantomData<P>,
    _odbc_item: PhantomData<O>,
    odbc_to_arrow: F,
}

impl<P, O, F> NullableStrategy<P, O, F> {
    fn new(odbc_to_arrow: F) -> Self {
        Self {
            _primitive_type: PhantomData,
            _odbc_item: PhantomData,
            odbc_to_arrow,
        }
    }
}

impl<P, O, F> ReadStrategy for NullableStrategy<P, O, F>
where
    P: Send + Sync + ArrowPrimitiveType,
    O: Send + Sync + Item,
    F: Send + Sync + Fn(&O) -> Result<P::Native, MappingError>,
{
    fn buffer_desc(&self) -> BufferDesc {
        O::buffer_desc(true)
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let opts = column_view.as_nullable_slice::<O>().unwrap();
        let mut builder = PrimitiveBuilder::<P>::with_capacity(opts.len());
        for odbc_opt in opts {
            builder.append_option(odbc_opt.map(&self.odbc_to_arrow).transpose()?);
        }
        Ok(Arc::new(builder.finish()))
    }
}

/// The source value returned from the ODBC datasource is out of range and can not be mapped into
/// its Arrow target type.
#[derive(Error, Debug)]
pub enum MappingError {
    #[error(
        "\
        Timestamp is not representable in arrow: {value}\n\
        Timestamps with nanoseconds precision are represented using a signed 64 Bit integer. This \
        limits their range to values between 1677-09-21 00:12:44 and \
        2262-04-11 23:47:16.854775807. The value returned from the database is outside of this \
        range. Suggestions to fix this error either reduce the precision or fetch the values as \
        text.\
    "
    )]
    OutOfRangeTimestampNs { value: NaiveDateTime },
}
