# tests/test_radius_sender.py
import os
import tempfile
import pytest
from pipeline.ingestion.radius_udp_sender import send_csv_as_radius


def test_send_csv_as_radius_accepts_num_sockets():
    """Verify that send_csv_as_radius accepts num_sockets argument and executes without signature errors."""
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as tmp:
        tmp.write("msisdn,acct_status_type,framed_ip\n")
        tmp.write("+84901234567,start,10.0.0.1\n")
        tmp_path = tmp.name

    try:
        # Test sending with num_sockets=4 to loopback on unused port
        send_csv_as_radius(
            csv_path=tmp_path,
            host="127.0.0.1",
            port=18133,  # mock/unused port
            rate=100.0,
            num_sockets=4,
            max_packets=1,
        )
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def test_sender_parameter_validation():
    """Verify ValueError is raised on invalid sender configurations."""
    with pytest.raises(ValueError, match="queue_size"):
        send_csv_as_radius(csv_path="fake.csv", queue_size=0)


def test_send_csv_as_radius_multi_socket():
    """Verify that the fire-and-forget sender can use multiple UDP sockets."""
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as tmp:
        tmp.write("msisdn,acct_status_type,framed_ip\n")
        tmp.write("+84901234567,start,10.0.0.1\n")
        tmp_path = tmp.name

    try:
        send_csv_as_radius(
            csv_path=tmp_path,
            host="127.0.0.1",
            port=18134,
            rate=100.0,
            num_sockets=4,
            max_packets=2,
        )
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
