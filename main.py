import os
import csv
import requests
import threading

from datetime import datetime
from typing_extensions import Any, Generator
from api import login, get_datafields, get_alpha_result, multi_simulate
from generate_alphas import generate_alphas_save_to_csv

email = os.environ.get("WQ_EMAIL")
password = os.environ.get("WQ_PASSWORD")
alpha_csv_filename = f"alphas_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"
result_csv_filename = f"results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"

alpha_gen_lock = threading.Lock()
result_csv_lock = threading.Lock()


def get_current_time() -> str:
    """Return current time as a string."""
    return datetime.now().strftime("%H:%M:%S")


def import_csv_lines(filename: str, delimiter="|") -> list[str] | None:
    """Import CSV lines into a list of strings."""

    lines = []
    if len(delimiter) != 1:
        print("CSV delimiter must be a single character!")
        return None

    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for row in reader:
            row = ''.join(row)
            lines.append(row)
    return lines


def yield_csv_lines(filename: str, delimiter="|") -> Generator[str, Any, None]:
    """Yield CSV lines one by one."""

    if len(delimiter) != 1:
        print("CSV delimiter must be a single character!")
        return

    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for row in reader:
            row = ''.join(row)
            yield row


def export_result_dict_to_csv(filename: str, result_dict: dict[str, int | float | str], delimiter="|") -> None:
    """Export a dictionary into a CSV file."""

    if len(delimiter) != 1:
        print("CSV delimiter must be a single character!")
        return None

    if not os.path.exists(filename):
        with open(filename, "a") as f:
            for head in result_dict.keys():
                f.write(f"{head} {delimiter} ")
            f.write("\n")

    with open(filename, "a") as f:
        for val in result_dict.values():
            f.write(f"{val} {delimiter} ")
        f.write("\n")
    return None


def continuous_multi_simulate(s: requests.Session, alpha_gen: Generator, result_csv_filename: str, region: str, universe: str, delay: int, decay: int, neutralization: str, truncation: float, pasteurization="ON", testPeriod="P0Y0M", unitHandling="VERIFY", nanHandling="ON", maxTrade="OFF", maxRetries=3, batch_size=10) -> None:
    """Continuously multi-simulate alphas, saving results to a CSV file."""

    while True:
        with alpha_gen_lock:
            alphas = []
            for _ in range(batch_size):
                try:
                    alphas.append(next(alpha_gen))
                except StopIteration:
                    break

        alphaIDs = multi_simulate(s, alphas=alphas, region=region, universe=universe, delay=delay, decay=decay, neutralization=neutralization, truncation=truncation, pasteurization=pasteurization, testPeriod=testPeriod, unitHandling=unitHandling, nanHandling=nanHandling, maxTrade=maxTrade, maxRetries=maxRetries)
        if alphaIDs is None:
            continue

        for alphaID in alphaIDs:
            result_dict = dict()
            result_dict["*alphaID"] = alphaID
            result_dict = get_alpha_result(s, alphaID, maxRetries=3)
            if result_dict is None:
                continue
            with result_csv_lock:
                export_result_dict_to_csv(filename=result_csv_filename, result_dict=result_dict, delimiter="|")
        print(f"[INFO {get_current_time()}] Saved results to CSV file successfully.")


def main(max_concurrent=8) -> None:
    """Main workflow for automatic alpha testing."""

    auth_session = login(email=email, password=password)
    print(f"[INFO {get_current_time()}] Logged in successfully.")
    fields_dict = get_datafields(auth_session, datasetID="fundamental3", region="USA", dataType="MATRIX", universe="TOP3000", delay=1)

    generate_alphas_save_to_csv(filename=alpha_csv_filename, field_dict=fields_dict, amount=2_500)
    alpha_gen = yield_csv_lines(filename=alpha_csv_filename, delimiter="|")

    threads = []
    print(f"[INFO {get_current_time()}] Started multi-simulating with {max_concurrent} threads.")
    for _ in range(max_concurrent):
        t = threading.Thread(target=continuous_multi_simulate, args=(auth_session, alpha_gen, result_csv_filename, "USA", "TOP3000", 1, 0, "CROWDING", 0.04))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
