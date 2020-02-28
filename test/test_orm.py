from simplesqlite import connect_memdb
from simplesqlite.model import Blob, Integer, Model, Real, Text
from simplesqlite.query import Where


class Hoge(Model):
    hoge_id = Integer()
    name = Text()


class Foo(Model):
    foo_id = Integer(not_null=True)
    name = Text(not_null=True)
    value = Real(not_null=True)
    blob = Blob()


def test_orm():
    con = connect_memdb()

    Hoge.attach(con, is_hidden=True)
    Hoge.create()
    assert Hoge.fetch_num_records() == 0
    hoge_inputs = [Hoge(hoge_id=10, name="a"), Hoge(hoge_id=20, name="b")]
    for hoge_input in hoge_inputs:
        Hoge.insert(hoge_input)

    Foo.attach(con)
    Foo.create()
    foo_inputs = [Foo(foo_id=11, name="aq", value=0.1), Foo(foo_id=22, name="bb", value=1.11)]
    for foo_input in foo_inputs:
        Foo.insert(foo_input)

    assert Hoge.fetch_num_records() == 2
    for record, hoge_input in zip(Hoge.select(), hoge_inputs):
        assert record == hoge_input

    for record, foo_input in zip(Foo.select(), foo_inputs):
        assert record == foo_input

    result = Hoge.select(where=Where("hoge_id", 999))
    assert len(list(result)) == 0
