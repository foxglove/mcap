use std::{borrow::Cow, collections::HashMap, sync::Arc};

use crate::{records, Channel, McapError, McapResult, Schema};

/// Collect raw [`records::Channel`] and [`records::SchemaHeader`] and wire them together into owned
/// [`Channel`] structs.
///
/// This can be useful for consumers of the MCAP library creating their own readers.
#[derive(Debug, Default)]
pub struct ChannelAccumulator<'a> {
    pub(crate) schemas: HashMap<u16, Arc<Schema<'a>>>,
    pub(crate) channels: HashMap<u16, Arc<Channel<'a>>>,
}

impl<'a> ChannelAccumulator<'a> {
    /// Add a new schema into the accumulator, to be referenced by future channels.
    ///
    /// Call this method before [`ChannelAccumulator::add_channel`].
    pub fn add_schema(
        &mut self,
        header: records::SchemaHeader,
        data: Cow<'a, [u8]>,
    ) -> McapResult<()> {
        if header.id == 0 {
            return Err(McapError::InvalidSchemaId);
        }

        let schema = Schema {
            name: header.name,
            encoding: header.encoding,
            data,
        };

        if let Some(preexisting) = self.schemas.get(&header.id) {
            if **preexisting != schema {
                return Err(McapError::ConflictingSchemas(schema.name));
            }

            // since the schema for the provided id already exists and is identical, return ok
            return Ok(());
        }

        self.schemas.insert(header.id, Arc::new(schema));

        Ok(())
    }

    /// Add a new channel into the accumulator, wiring it up with its previously added [`Schema`].
    ///
    /// This method will return an error if:
    /// - the schema referenced by the channel does not exist
    /// - a conflicting version of the current channel already exists
    pub fn add_channel(&mut self, chan: records::Channel) -> McapResult<()> {
        // The schema ID can be 0 for "no schema",
        // Or must reference some previously-read schema.
        let schema = if chan.schema_id == 0 {
            None
        } else {
            match self.schemas.get(&chan.schema_id) {
                Some(s) => Some(s.clone()),
                None => {
                    return Err(McapError::UnknownSchema(chan.topic, chan.schema_id));
                }
            }
        };

        let channel = Channel {
            topic: chan.topic.clone(),
            schema,
            message_encoding: chan.message_encoding,
            metadata: chan.metadata,
        };

        // check for an existing channel with the provided id
        if let Some(preexisting) = self.channels.get(&chan.id) {
            if **preexisting != channel {
                return Err(McapError::ConflictingChannels(chan.topic));
            }

            // since the channel for the provided id already exists and is identical, return ok
            return Ok(());
        }

        self.channels.insert(chan.id, Arc::new(channel));

        Ok(())
    }

    /// Get a specific channel by id
    pub fn get(&self, chan_id: u16) -> Option<Arc<Channel<'a>>> {
        self.channels.get(&chan_id).cloned()
    }

    /// Create an iterator over all channels
    pub fn channels(&self) -> impl Iterator<Item = &Arc<Channel>> {
        self.channels.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_override_schema_ptr_same() {
        let mut accumulator = ChannelAccumulator::default();

        accumulator
            .add_schema(
                records::SchemaHeader {
                    id: 1,
                    name: "great_schema".into(),
                    encoding: "great_encoding".into(),
                },
                Cow::from(vec![]),
            )
            .expect("should insert");

        let first_schema = accumulator.schemas.get(&1).expect("should exist").clone();

        // add an identical schema to the first one
        accumulator
            .add_schema(
                records::SchemaHeader {
                    id: 1,
                    name: "great_schema".into(),
                    encoding: "great_encoding".into(),
                },
                Cow::from(vec![]),
            )
            .expect("should insert");

        let second_schema = accumulator.schemas.get(&1).expect("should exist").clone();

        assert!(Arc::ptr_eq(&first_schema, &second_schema));
    }

    #[test]
    fn test_override_channel_ptr_same() {
        let mut accumulator = ChannelAccumulator::default();

        accumulator
            .add_schema(
                records::SchemaHeader {
                    id: 1,
                    name: "great_schema".into(),
                    encoding: "great_encoding".into(),
                },
                Cow::from(vec![]),
            )
            .expect("should insert");

        accumulator
            .add_channel(records::Channel {
                id: 1,
                schema_id: 1,
                topic: "great_topic".into(),
                metadata: Default::default(),
                message_encoding: "great_encoding".into(),
            })
            .expect("should insert");

        let first_channel = accumulator.get(1).expect("should exist").clone();

        // add an identical channel to the first one
        accumulator
            .add_channel(records::Channel {
                id: 1,
                schema_id: 1,
                topic: "great_topic".into(),
                metadata: Default::default(),
                message_encoding: "great_encoding".into(),
            })
            .expect("should insert");

        let second_channel = accumulator.get(1).expect("should exist").clone();

        assert!(Arc::ptr_eq(&first_channel, &second_channel));
    }
}
