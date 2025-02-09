from lstore.page import Page


def test_page():
    print("Starting Tests")

    page = Page()

    # WRITE
    print("\n[TEST] Writing data to Page")
    offsets = []
    for i in range(page.max_records):
        offset = page.write(i * 10) 
        if offset == -1:
            print(f"failed at {i}")
            return
        offsets.append(offset)

    print("pass")

    # READ
    print("\n[TEST] Reading data from Page")
    for i, offset in enumerate(offsets):
        value = page.read(offset)
        expected = i * 10
        assert value == expected, f"Read error: expected {expected}, got {value}"
    
    print("pass")

    # UPDATE
    print("\n[TEST] Updating data in Page")
    for i, offset in enumerate(offsets):
        new_value = (i + 1) * 100  # 100, 200, 300, ...
        success = page.update(offset, new_value)
        assert success, f"Update failed at offset {offset}"
        value = page.read(offset)
        assert value == new_value, f"Update error: expected {new_value}, got {value}"
    
    print("pass")

    # EDGE TEST
    print("\n[TEST] Edge cases")

    # WRITE OVER PAGE SIZEE
    result = page.write(999)
    assert result == -1, "Edge case failed: Page should be full but accepted new data"

    # READ ILLEGAL OFFSET
    value = page.read(-8)
    assert value is -1, "Edge case failed: Read from negative offset should return None"

    value = page.read(page.PAGE_SIZE)
    assert value is -1, "Edge case failed: Read from out-of-bounds offset should return None"

    # UPDATE ILLEGAL OFFSET
    result = page.update(-8, 777)
    assert not result, "Edge case failed: Update at negative offset should fail"

    result = page.update(page.PAGE_SIZE, 777)
    assert not result, "Edge case failed: Update at out-of-bounds offset should fail"

    print("pass")

    print("All tests pass")

# 运行测试
test_page()
