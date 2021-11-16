#![allow(clippy::rc_buffer)]

use std::ptr::null_mut;
use std::slice;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Weak};

use crate::HashMap;

use crate::column::ColumnIndex;
use crate::error::Error;
use crate::ext::ustr::UStr;
use crate::row::Row;
use crate::sqlite::statement::{StatementHandle, StatementHandleRef};
use crate::sqlite::{Sqlite, SqliteColumn, SqliteValue, SqliteValueRef};

/// Implementation of [`Row`] for SQLite.
pub struct SqliteRow {
    pub(crate) values: Box<[SqliteValue]>,
    pub(crate) columns: Arc<Vec<SqliteColumn>>,
    pub(crate) column_names: Arc<HashMap<UStr, usize>>,
}

impl crate::row::private_row::Sealed for SqliteRow {}

// Accessing values from the statement object is
// safe across threads as long as we don't call [sqlite3_step]

// we block ourselves from doing that by only exposing
// a set interface on [StatementHandle]

unsafe impl Send for SqliteRow {}
unsafe impl Sync for SqliteRow {}

impl SqliteRow {
    pub(crate) fn inflated(
        statement: &StatementHandle,
        columns: &Arc<Vec<SqliteColumn>>,
        column_names: &Arc<HashMap<UStr, usize>>,
    ) -> Self {
        let size = statement.column_count();
        let mut values = Vec::with_capacity(size);

        for i in 0..size {
            values.push(unsafe {
                let raw = statement.column_value(i);

                SqliteValue::new(raw, columns[i].type_info.clone())
            });
        }

        Self {
            values: values.into_boxed_slice(),
            columns: Arc::clone(columns),
            column_names: Arc::clone(column_names),
        }
    }

    // creates a new row that is internally referencing the **current** state of the statement
    // returns a weak reference to an atomic list where the executor should inflate if its going
    // to increment the statement with [step]
    pub(crate) fn current(
        statement: StatementHandleRef,
        columns: &Arc<Vec<SqliteColumn>>,
        column_names: &Arc<HashMap<UStr, usize>>,
    ) -> (Self, Weak<AtomicPtr<SqliteValue>>) {
        unimplemented!()
    }

    // inflates this Row into memory as a list of owned, protected SQLite value objects
    // this is called by the
    #[allow(clippy::needless_range_loop)]
    pub(crate) fn inflate(
        statement: &StatementHandle,
        columns: &[SqliteColumn],
        values_ref: &AtomicPtr<SqliteValue>,
    ) {
        unimplemented!()
    }

    pub(crate) fn inflate_if_needed(
        statement: &StatementHandle,
        columns: &[SqliteColumn],
        weak_values_ref: Option<Weak<AtomicPtr<SqliteValue>>>,
    ) {
        unimplemented!()
    }
}

impl Row for SqliteRow {
    type Database = Sqlite;

    fn columns(&self) -> &[SqliteColumn] {
        &self.columns
    }

    fn try_get_raw<I>(&self, index: I) -> Result<SqliteValueRef<'_>, Error>
    where
        I: ColumnIndex<Self>,
    {
        let index = index.index(self)?;
        Ok(SqliteValueRef::value(&self.values[index]))
    }
}

impl ColumnIndex<SqliteRow> for &'_ str {
    fn index(&self, row: &SqliteRow) -> Result<usize, Error> {
        row.column_names
            .get(*self)
            .ok_or_else(|| Error::ColumnNotFound((*self).into()))
            .map(|v| *v)
    }
}

#[cfg(feature = "any")]
impl From<SqliteRow> for crate::any::AnyRow {
    #[inline]
    fn from(row: SqliteRow) -> Self {
        crate::any::AnyRow {
            columns: row.columns.iter().map(|col| col.clone().into()).collect(),
            kind: crate::any::row::AnyRowKind::Sqlite(row),
        }
    }
}
