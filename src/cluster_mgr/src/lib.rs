#![feature(variant_count)]
#![feature(async_closure)]
#![feature(proc_macro_hygiene)]
#![feature(stmt_expr_attributes)]
#![feature(coroutines)]
// #![feature(lazy_cell)]
// #![feature(associated_type_bounds)]
// #![feature(type_alias_impl_trait)]

extern crate core;

use chrono::Utc;
use std::fmt::Debug;
use strum_macros::AsRefStr;

pub mod cli;
pub mod config;
pub mod state;

#[macro_export]
macro_rules! enum_into_trait {
    ($trait_name:tt, $func_name:ident, $enum_type:ty) => {
        pub trait $trait_name: std::fmt::Debug {
            fn $func_name(arg_value: $enum_type) -> Self
            where
                Self: Sized;
        }
    };
}

pub trait Stateful: Debug {
    fn to_values(&self) -> Vec<StateValue>;
}

#[derive(Clone, Debug, Eq, PartialEq, AsRefStr)]
pub enum StateValue {
    #[strum(serialize = "String")]
    Varchar(String),
    #[strum(serialize = "i32")]
    Integer(i32),
    #[strum(serialize = "i64")]
    Bigint(i64),
    #[strum(serialize = "chrono::DateTime<Utc>")]
    Timestamp(chrono::DateTime<Utc>),
}

enum_into_trait!(StateTypeInto, type_value, StateValue);

impl StateValue {
    pub fn into_inner_value<T: StateTypeInto>(self) -> T {
        StateTypeInto::type_value(self)
    }
}
