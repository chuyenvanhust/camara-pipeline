import pytest
from pydantic import BaseModel, ValidationError
from api.schemas.common import PhoneNumber, ErrorResponse

# Định nghĩa một Pydantic Model giả lập để bọc PhoneNumber khi test
class TargetModel(BaseModel):
    phone_number: PhoneNumber

# -----------------------------------------------------------------
# Kịch bản 1: Số điện thoại hợp lệ theo chuẩn ITU-T E.164
# -----------------------------------------------------------------
@pytest.mark.parametrize("valid_phone", [
    "+84912345678",    # Định dạng Việt Nam tiêu chuẩn
    "+14155552671",    # Định dạng Mỹ (Mã quốc gia 1 chữ số)
    "+442079460958",   # Định dạng Anh (Mã quốc gia 2 chữ số)
    "+263771234567",   # Định dạng Zimbabwe (Mã quốc gia 3 chữ số)
    "+998901234567"    # Đúng giới hạn biên độ dài tối đa (15 ký tự bao gồm cả số)
])
def test_phone_number_valid_e164(valid_phone):
    """ Đảm bảo các số điện thoại chuẩn E.164 được parse thành công """
    model = TargetModel(phone_number=valid_phone)
    assert model.phone_number == valid_phone
    assert isinstance(model.phone_number, PhoneNumber)

# -----------------------------------------------------------------
# Kịch bản 2: Số điện thoại SAI định dạng (Bẫy lỗi biên)
# -----------------------------------------------------------------
@pytest.mark.parametrize("invalid_phone", [
    "0912345678",        # Lỗi: Thiếu dấu '+' ở đầu
    "+84912",            # Lỗi: Quá ngắn (Ít hơn 7 chữ số)
    "+1234567890123456", # Lỗi: Quá dài (Vượt quá 15 chữ số)
    "+84 912345678",     # Lỗi: Chứa khoảng trắng ở giữa số
    "+84912-345-678",    # Lỗi: Chứa ký tự đặc biệt gạch ngang
    "+0123456789",       # Lỗi: Mã quốc gia bắt đầu bằng số 0 (E.164 quy định mã quốc gia từ [1-9])
    "abc+84912345"       # Lỗi: Chứa ký tự chữ cái
])
def test_phone_number_invalid_formats(invalid_phone):
    """ Đảm bảo Pydantic tự động chặn đứng và ném lỗi ValidationError khi gặp format sai """
    with pytest.raises(ValidationError) as exc_info:
        TargetModel(phone_number=invalid_phone)
    
    # Kiểm tra xem thông báo lỗi có chỉ ra lỗi do định dạng PhoneNumber hay không
    assert "phone_number" in str(exc_info.value)

# -----------------------------------------------------------------
# Kịch bản 3: Kiểm thử cấu trúc ErrorResponse chuẩn
# -----------------------------------------------------------------
def test_error_response_structure():
    """ Đảm bảo ErrorResponse khởi tạo đúng các trường dữ liệu bắt buộc """
    error_data = {
        "error": "INVALID_MSISDN",
        "message": "The provided MSISDN does not comply with E.164 validation rules.",
        "request_id": "550e8400-e29b-41d4-a716-446655440000"
    }
    
    response = ErrorResponse(**error_data)
    assert response.error == "INVALID_MSISDN"
    assert response.message == error_data["message"]
    assert response.request_id == error_data["request_id"]

def test_error_response_missing_fields():
    """ Đảm bảo ErrorResponse ném lỗi nếu thiếu bất kỳ trường bắt buộc nào """
    with pytest.raises(ValidationError):
        # Thiếu trường request_id
        ErrorResponse(error="BAD_REQUEST", message="Missing internal details")