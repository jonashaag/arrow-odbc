use std::{char::decode_utf16, convert::TryInto, sync::Arc, cmp::min};

use arrow::array::{ArrayRef, StringBuilder};
use odbc_api::{
    buffers::{AnySlice, BufferDesc},
    DataType as OdbcDataType,
};

use super::{ColumnFailure, MappingError, ReadStrategy};

/// This function decides wether this column will be queried as narrow (assumed to be utf-8) or
/// wide text (assumed to be utf-16). The reason we do not always use narrow is that the encoding
/// dependends on the system locals which is usually not UTF-8 on windows systems. Furthermore we
/// are trying to adapt the buffer size to the maximum string length the column could contain.
pub fn choose_text_strategy(
    sql_type: OdbcDataType,
    lazy_display_size: impl FnMut() -> Result<isize, odbc_api::Error>,
    max_text_size: Option<usize>,
) -> Result<Box<dyn ReadStrategy>, ColumnFailure> {
    let is_narrow = matches!(
        sql_type,
        OdbcDataType::LongVarchar { .. } | OdbcDataType::Varchar { .. } | OdbcDataType::Char { .. }
    );
    let is_wide = matches!(
        sql_type,
        OdbcDataType::WVarchar { .. } | OdbcDataType::WChar { .. }
    );
    let is_text = is_narrow || is_wide;
    let apply_buffer_limit = |len| match (len, max_text_size) {
        (0, None) => Err(ColumnFailure::ZeroSizedColumn { sql_type }),
        (0, Some(limit)) => Ok(limit),
        (len, None) => Ok(len),
        (len, Some(limit)) => Ok(min(len, limit)),
    };
    let strategy = if is_text {
        if cfg!(target_os = "windows") {
            let hex_len = sql_type.utf16_len().unwrap();
            let hex_len = apply_buffer_limit(hex_len)?;
            wide_text_strategy(hex_len)
        } else {
            let octet_len = sql_type.utf8_len().unwrap();
            let octet_len = apply_buffer_limit(octet_len)?;
            narrow_text_strategy(octet_len)
        }
    } else {
        let display_size: usize = sql_type
            .display_size()
            .map(|ds| Ok(ds as isize))
            .unwrap_or_else(lazy_display_size)
            .map_err(|source| ColumnFailure::UnknownStringLength { sql_type, source })?
            .try_into()
            .unwrap();

        let display_size = apply_buffer_limit(display_size)?;

        // We assume non text type colmuns to only consist of ASCII characters.
        narrow_text_strategy(display_size)
    };

    Ok(strategy)
}

fn wide_text_strategy(u16_len: usize) -> Box<dyn ReadStrategy> {
    Box::new(WideText::new(u16_len))
}

fn narrow_text_strategy(octet_len: usize) -> Box<dyn ReadStrategy> {
    Box::new(NarrowText::new(octet_len))
}

/// Strategy requesting the text from the database as UTF-16 (Wide characters) and emmitting it as
/// UTF-8. We use it, since the narrow representation in ODBC is not always guaranteed to be UTF-8,
/// but depends on the local instead.
pub struct WideText {
    /// Maximum string length in u16, excluding terminating zero
    max_str_len: usize,
}

impl WideText {
    pub fn new(max_str_len: usize) -> Self {
        Self { max_str_len }
    }
}

impl ReadStrategy for WideText {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::WText {
            max_str_len: self.max_str_len,
        }
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let view = column_view.as_w_text_view().unwrap();
        let item_capacity = view.len();
        // Any utf-16 character could take up to 4 Bytes if represented as utf-8, but since mostly
        // this is 1 to one, and also not every string is likeyl to use its maximum capacity, we
        // rather accept the reallocation in these scenarios.
        let data_capacity = self.max_str_len * item_capacity;
        let mut builder = StringBuilder::with_capacity(item_capacity, data_capacity);
        // Buffer used to convert individual values from utf16 to utf8.
        let mut buf_utf8 = String::new();
        for value in view.iter() {
            buf_utf8.clear();
            let opt = if let Some(utf16) = value {
                for c in decode_utf16(utf16.as_slice().iter().cloned()) {
                    buf_utf8.push(c.unwrap());
                }
                Some(&buf_utf8)
            } else {
                None
            };
            builder.append_option(opt);
        }
        Ok(Arc::new(builder.finish()))
    }
}

pub struct NarrowText {
    /// Maximum string length in u8, excluding terminating zero
    max_str_len: usize,
}

impl NarrowText {
    pub fn new(max_str_len: usize) -> Self {
        Self { max_str_len }
    }
}

impl ReadStrategy for NarrowText {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Text {
            max_str_len: self.max_str_len,
        }
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let view = column_view.as_text_view().unwrap();
        let mut builder = StringBuilder::with_capacity(view.len(), self.max_str_len * view.len());
        for value in view.iter() {
            builder.append_option(value.map(|bytes| {
                std::str::from_utf8(bytes)
                    .expect("ODBC column had been expected to return valid utf8, but did not.")
            }));
        }
        Ok(Arc::new(builder.finish()))
    }
}
