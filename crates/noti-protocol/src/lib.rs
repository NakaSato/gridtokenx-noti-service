#![allow(unsafe_code)]
#![allow(clippy::all)]
#![allow(clippy::pedantic)]

//! # noti-protocol
//!
//! Generated protobuf / `ConnectRPC` types for the notification service.

pub mod noti {
    include!(concat!(env!("OUT_DIR"), "/_noti_include.rs"));
    pub use noti::*;
}
