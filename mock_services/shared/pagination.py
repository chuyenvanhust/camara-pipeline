#pagination.py
from typing import Generic, TypeVar, List
from pydantic import BaseModel, Field

# Định nghĩa Generic Type biến đổi linh hoạt theo Model dữ liệu truyền vào
T = TypeVar('T')

# -----------------------------------------------------------------
# 1. Schema Phân trang dùng chung (Generic Pagination)
# -----------------------------------------------------------------
class Page(BaseModel, Generic[T]):
    items: List[T] = Field(..., description="Danh sách dữ liệu của trang hiện tại")
    total: int     = Field(..., description="Tổng số bản ghi có trong hệ thống")
    page: int      = Field(..., description="Số thứ tự trang hiện tại (1-indexed)")
    pages: int     = Field(..., description="Tổng số trang tính được dựa trên limit")

# -----------------------------------------------------------------
# 2. Helper xử lý logic tính toán Phân trang
# -----------------------------------------------------------------
def paginate_records(all_records: List[T], page: int, limit: int) -> Page[T]:
    """
    Trích xuất danh sách con (slice) từ mảng tổng và tính toán các chỉ số phân trang.
    Gợi ý công thức:
        - offset = (page - 1) * limit
        - pages = ceil(total / limit)
    """
    # TODO: Kiểm tra điều kiện đầu vào của page và limit (phải > 0)
    if page < 1 or limit < 1:
        raise ValueError("Page and limit must be greater than 0.")
    # TODO: Tính toán offset và cắt mảng dữ liệu (all_records[offset : offset + limit])
    offset = (page - 1) * limit
    paginated_items = all_records[offset : offset + limit]
    total_records = len(all_records)

    # TODO: Tính tổng số trang (pages)
    pages = (total_records + limit - 1) // limit  
    # TODO: Trả về đối tượng Page[T] hoàn chỉnh
    return Page[T](
        items=paginated_items,
        total=total_records,
        page=page,
        pages=pages
    )
    