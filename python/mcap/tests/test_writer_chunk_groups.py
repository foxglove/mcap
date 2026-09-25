from io import BytesIO

import pytest

from mcap.reader import NonSeekingReader, make_reader
from mcap.writer import Writer


def test_channels_in_different_groups_never_share_a_chunk():
    output = BytesIO()
    writer = Writer(output, chunk_size=256)
    writer.start()
    schema_id = writer.register_schema(name="s", encoding="jsonschema", data=b"{}")
    camera = writer.register_channel(
        "/camera", "json", schema_id, chunk_group="cameras"
    )
    joints = writer.register_channel("/joints", "json", schema_id, chunk_group="state")
    gripper = writer.register_channel(
        "/gripper", "json", schema_id, chunk_group="state"
    )
    for i in range(100):
        writer.add_message(camera, log_time=i, data=b"{}", publish_time=i)
        writer.add_message(joints, log_time=i, data=b"{}", publish_time=i)
        writer.add_message(gripper, log_time=i, data=b"{}", publish_time=i)
    writer.finish()

    reader = make_reader(BytesIO(output.getvalue()))
    summary = reader.get_summary()
    assert summary is not None
    chunk_channel_sets = {
        frozenset(chunk_index.message_index_offsets)
        for chunk_index in summary.chunk_indexes
    }
    assert len(summary.chunk_indexes) > 2
    assert chunk_channel_sets == {frozenset({camera}), frozenset({joints, gripper})}
    log_times = [message.log_time for _, _, message in reader.iter_messages()]
    assert log_times == sorted(log_times) and len(log_times) == 300


def test_each_group_carries_the_schemas_its_channels_use():
    # The schema is registered before any group exists, and the only channel using it is in
    # a group, so a reader streaming from the start sees it only if the group's chunk has it.
    output = BytesIO()
    writer = Writer(output)
    writer.start()
    schema_id = writer.register_schema(name="s", encoding="jsonschema", data=b"{}")
    channel_id = writer.register_channel(
        "/joints", "json", schema_id, chunk_group="state"
    )
    writer.add_message(channel_id, log_time=0, data=b"{}", publish_time=0)
    writer.finish()

    messages = list(NonSeekingReader(BytesIO(output.getvalue())).iter_messages())
    assert len(messages) == 1
    schema, _, _ = messages[0]
    assert schema is not None and schema.id == schema_id


def test_chunk_group_requires_chunking():
    writer = Writer(BytesIO(), use_chunking=False)
    writer.start()
    with pytest.raises(ValueError):
        writer.register_channel("/joints", "json", 0, chunk_group="state")
