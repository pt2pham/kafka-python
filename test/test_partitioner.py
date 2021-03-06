from __future__ import absolute_import

import pytest

from kafka.partitioner import DefaultPartitioner, Murmur2Partitioner, RoundRobinPartitioner
from kafka.partitioner.hashed import murmur2


def test_default_partitioner():
    partitioner = DefaultPartitioner()
    all_partitions = available = list(range(100))
    # partitioner should return the same partition for the same key
    p1 = partitioner(b'foo', all_partitions, available)
    p2 = partitioner(b'foo', all_partitions, available)
    assert p1 == p2
    assert p1 in all_partitions

    # when key is None, choose one of available partitions
    assert partitioner(None, all_partitions, [123]) == 123

    # with fallback to all_partitions
    assert partitioner(None, all_partitions, []) in all_partitions


def test_roundrobin_partitioner():
    partitioner = RoundRobinPartitioner()
    all_partitions = available = list(range(100))
    # partitioner should cycle between partitions
    i = 0
    max_partition = all_partitions[len(all_partitions) - 1]
    while i <= max_partition:
        assert i == partitioner(None, all_partitions, available)
        i += 1

    i = 0
    while i <= int(max_partition / 2):
        assert i == partitioner(None, all_partitions, available)
        i += 1

    # test dynamic partition re-assignment
    available = available[:-25]

    while i <= max(available):
        assert i == partitioner(None, all_partitions, available)
        i += 1

    all_partitions = list(range(200))
    available = all_partitions

    max_partition = all_partitions[len(all_partitions) - 1]
    while i <= max_partition:
        assert i == partitioner(None, all_partitions, available)
        i += 1


@pytest.mark.parametrize("bytes_payload,partition_number", [
    (b'', 681), (b'a', 524), (b'ab', 434), (b'abc', 107), (b'123456789', 566),
    (b'\x00 ', 742)
])
def test_murmur2_java_compatibility(bytes_payload, partition_number):
    p = Murmur2Partitioner(range(1000))
    # compare with output from Kafka's org.apache.kafka.clients.producer.Partitioner
    assert p.partition(bytes_payload) == partition_number


def test_murmur2_not_ascii():
    # Verify no regression of murmur2() bug encoding py2 bytes that don't ascii encode
    murmur2(b'\xa4')
    murmur2(b'\x81' * 1000)
