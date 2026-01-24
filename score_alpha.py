import requests
import numpy as np
import math
import warnings
import time

from datetime import datetime
from misc import patched_session_request

base_url = "https://api.worldquantbrain.com"
alpha_url = base_url + "/alphas"

requests.Session.request = patched_session_request


def get_current_time() -> str:
    """Return current time as a string."""
    return datetime.now().strftime("%H:%M:%S")


def get_alpha_score(session: requests.Session | None, alpha_id: str) -> float:
    """
    Given an alpha a score according to the metrics defined in the function.
    :param session: REQUIRED. Your ``requests.Session`` object
    :param alpha_id: REQUIRED. Your alpha's ID
    """

    yearly_url = f"{alpha_url}/{alpha_id}/recordsets/yearly-stats"
    this_alpha_url = f"{alpha_url}/{alpha_id}"

    if session is None:
        warnings.warn(f"[INFO {get_current_time()}] No session provided / session unauthorized, returning -1.")
        return -1
    else:
        while True:
            try:
                stats = session.get(yearly_url).json()["records"][: -1]
                is_result = session.get(this_alpha_url).json()["is"]
                break
            except requests.exceptions.JSONDecodeError:
                time.sleep(1)
            except requests.exceptions.RequestException:
                time.sleep(1)

        long, short, sharpe, returns, drawdown = (np.array([]) for _ in range(5))
        total_returns, total_drawdown = is_result["returns"], is_result["drawdown"]
        for stat in stats:
            long = np.append(long, stat[3])
            short = np.append(short, stat[4])
            sharpe = np.append(sharpe, stat[6])
            returns = np.append(returns, stat[7])
            drawdown = np.append(drawdown, stat[8])

        # Test 1
        # Take the function 2x/(1+x^2). Maps from [0, +inf) to [0, 1], and output the same value for reciprocals. Average this score over the whole IS period.
        x1 = np.where((short != 0) & (long != 0), long / short, 0)
        score1 = np.mean(2 * x1 / (1 + x1 ** 2))

        # Test 2
        # Take the function e^-0.25x. Maps from [0, +inf) to [0, 1].
        x2 = np.std(sharpe)
        score2 = math.exp(-0.25 * x2)

        # Test 3
        # Take the function e^-0.25x. Maps from [0, +inf) to [0, 1].
        x3 = np.std(returns)
        score3 = math.exp(-0.25 * x3)

        # Test 4
        # The ratio of years with sharpe > 1
        score4 = np.mean(sharpe > 1)

        # Test 5
        # Take the function tanh(x). Maps from (0, +inf) to (0, 1).
        score5 = math.tanh(total_returns / total_drawdown) if total_drawdown > 0 else 0

        return round(0.2 * (score1 + score2 + score3 + score4 + score5), 4)