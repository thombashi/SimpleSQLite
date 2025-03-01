from simplesqlite.error import OperationalError


def test_operational_error_str_with_message():
    error_message = "Test error message"
    error = OperationalError(message=error_message)
    assert str(error) == error_message


def test_operational_error_str_without_message():
    error = OperationalError()
    assert str(error) == ""
