import random
from typing import List


def generate_tac_codes(seed: int, count: int) -> List[str]:
    """
    Sinh danh sách TAC 6 chữ số (000000-999999), duy nhất, deterministic
    theo seed. Dùng chung bởi gsma_tac/seed.py (ghi ra CSV) VÀ
    simulator/generator.py (fallback khi GSMA mock offline) — đảm bảo
    2 bên LUÔN ra cùng một tập TAC dù có gọi API hay không.

    Dùng random.Random(seed) cục bộ, KHÔNG đụng vào global `random` module,
    để không bị các class khác (ErrorInjector, RadiusDataGenerator...)
    reseed đè lên.
    """
    if count >= 1_000_000:
        raise ValueError("count vượt quá không gian TAC 6 chữ số (tối đa 1,000,000)")

    rng = random.Random(seed)
    used = set()
    tacs: List[str] = []
    while len(tacs) < count:
        tac = f"{rng.randint(0, 999999):06d}"
        if tac not in used:
            used.add(tac)
            tacs.append(tac)
    return tacs