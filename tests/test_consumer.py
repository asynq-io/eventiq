RET_VAL = 42


async def test_consumer_process(test_consumer, ce):
    res = await test_consumer.process(ce)
    assert res == RET_VAL


async def test_generic_consumer_process(generic_test_consumer, ce):
    res = await generic_test_consumer.process(ce)
    assert res == RET_VAL
