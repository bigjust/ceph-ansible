from . import ceph_crush

fake_osd_crush_location = {
    "host": "monhost",
    "root": "maroute",
    "rack": "monrack",
    "room": "maroom",
    "chassis": "monchassis",
    "pod": "monpod",
}

fake_cluster = "test"


class TestCephCrushModule(object):

    def test_create_and_move_buckets():
        result = create_and_move_buckets(fake_cluster, fake_osd_crush_location)
        assert fake_rules == result
