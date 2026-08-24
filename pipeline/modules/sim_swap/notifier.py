# pipeline/modules/sim_swap/notifier.py
"""
F-03: Notifier đã được tách khỏi hot path.

Consumer KHÔNG gọi HTTP trực tiếp nữa — chỉ ghi notification_log
với status='PENDING' trong cùng transaction với DB writes (xem consumer.py).

NotificationDispatcher (pipeline/dispatcher/notification_dispatcher.py)
sẽ poll notification_log và gửi HTTP callback riêng biệt.

File này giữ lại cho backward compatibility nhưng không còn được import
bởi consumer. Có thể xoá sau khi confirm dispatcher hoạt động ổn định.
"""
import logging

logger = logging.getLogger(__name__)
logger.info(
    "sim_swap/notifier.py: Module này đã deprecated. "
    "Notification được xử lý qua notification_dispatcher.py (outbox pattern)."
)
