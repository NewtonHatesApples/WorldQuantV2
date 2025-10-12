import requests
import warnings
import time
import random

base_url = "https://api.worldquantbrain.com"
login_url = base_url + "/authentication"
alpha_url = base_url + "/alphas"
simulation_url = base_url + "/simulations"
data_url = base_url + "/data-fields"


def login(email: str, password: str, maxRetries=3) -> requests.Session | None:
    """
    Login to WorldQuant.
    :param email: REQUIRED. Your email
    :param password: REQUIRED. Your password
    :param maxRetries: When provided, stop retry logging in to WorldQuant after this many retries. Default to 3
    :return: ``requests.Session`` or ``None``
    """

    if maxRetries <= 0:
        warnings.warn(f"{maxRetries} attempts exceeded. Stop retry logging in.")
        return None

    s = requests.Session()
    resp = s.post(login_url, auth=(email, password))

    if resp.status_code == requests.status_codes.codes.unauthorized:
        if resp.headers["WWW-Authenticate"] == "persona":  # Need biometric auth
            biometric_loc = resp.headers["Location"]
            input(
                f"Complete the biometric authentication at the following url: {base_url + biometric_loc}\nAfter finished, press Enter to continue ...")
            s.post(f"{base_url + biometric_loc}")
            resp2 = s.get(login_url)

            if resp2.status_code == 204:  # Failed biometric auth
                maxRetries -= 1
                print(f"Failed biometric authentication. Remaining attempts = {maxRetries}")
                return login(email, password, maxRetries)

            elif resp2.status_code == 200:  # Authorized
                return s

            else:  # Something went wrong, try again
                maxRetries -= 1
                print(f"Something went wrong. Remaining attempts = {maxRetries}")
                return login(email, password, maxRetries)

        else:  # Wrong username or password
            print("Wrong username or password.")
            return None

    elif resp.status_code == 200:  # Authorized
        return s

    else:  # Wrong username or password
        print("Wrong username or password.")
        return None


def get_datafields(s: requests.Session, datasetID: str, region: str, dataType: str, universe: str, delay: int, instrumentType="EQUITY", limit=50, theme=False, alphaCountLowerLimit=None, alphaCountUpperLimit=None, coverageLowerLimit=None, coverageUpperLimit=None, userCountLowerLimit=None, userCountUpperLimit=None, search=None) -> dict | None:
    """
    Get a dictionary of datafield names and their descriptions from WorldQuant.
    :param s: REQUIRED. Your ``requests.Session`` object
    :param datasetID: REQUIRED. e.g. `broker1`
    :param region: REQUIRED. e.g. `USA`
    :param dataType: REQUIRED. e.g. `VECTOR`
    :param universe: REQUIRED. e.g. `TOP3000`
    :param delay: REQUIRED. Either 0 or 1
    :param instrumentType: Must be `EQUITY` at this stage
    :param limit: Integer between 1 to 50 inclusive
    :param theme: either ``True`` or ``False``
    :param alphaCountLowerLimit: Positive Integer
    :param alphaCountUpperLimit: Positive Integer
    :param coverageLowerLimit: Float between 0 to 1
    :param coverageUpperLimit: Float between 0 to 1
    :param userCountLowerLimit: Positive Integer
    :param userCountUpperLimit: Positive Integer
    :param search: When provided, returns only datafields that contains this ``search`` string
    :return: A ``dict`` which contains both datafield names and descriptions
    """

    if limit <= 0 or limit > 50:
        warnings.warn("Limit must be between 1 and 50")
        return None

    offset = 0

    theme = "true" if theme else "false"
    if alphaCountLowerLimit is None: alphaCountLowerLimit = ""
    if alphaCountUpperLimit is None: alphaCountUpperLimit = ""
    if coverageLowerLimit is None: coverageLowerLimit = ""
    if coverageUpperLimit is None: coverageUpperLimit = ""
    if userCountLowerLimit is None: userCountLowerLimit = ""
    if userCountUpperLimit is None: userCountUpperLimit = ""
    if search is None: search = ""

    data = s.get(data_url, params={"dataset.id": datasetID, "region": region, "type": dataType, "universe": universe, "delay": delay, "instrumentType": instrumentType, "limit": limit, "theme": theme, "alpha>": alphaCountLowerLimit, "alpha<": alphaCountUpperLimit, "coverage>": coverageLowerLimit, "coverage<": coverageUpperLimit, "userCount>": userCountLowerLimit, "userCount<": userCountUpperLimit, "search": search}).json()
    count = data["count"]
    result_dict = {}

    for i in range(count // limit + 1):
        data = s.get(data_url, params={"dataset.id": datasetID, "region": region, "type": dataType, "universe": universe, "delay": delay, "instrumentType": instrumentType, "limit": limit, "theme": theme, "alpha>": alphaCountLowerLimit, "alpha<": alphaCountUpperLimit, "coverage>": coverageLowerLimit, "coverage<": coverageUpperLimit, "userCount>": userCountLowerLimit, "userCount<": userCountUpperLimit, "search": search, "offset": offset * i}).json()
        results = data["results"]
        for field in results:
            result_dict[field["id"]] = field["description"]

    return result_dict


def get_alpha_result(s: requests.Session, alphaID: str, maxRetries=3) -> dict | None:
    """
    Get the IS testing result of an alpha.
    :param s: REQUIRED. Your ``requests.Session`` object
    :param alphaID: REQUIRED. e.g. akr9MgER
    :param maxRetries: When provided, stop retry getting alpha result after this many retries. Default to 3
    :return: A ``dict`` which contains IS testing result
    """

    if maxRetries <= 0:
        warnings.warn(f"{maxRetries} attempts exceeded. Stop retry getting IS result of alpha {alphaID}.")
        return None

    is_result = s.get(alpha_url + f"/{alphaID}").json()
    return_dict = {}
    if len(is_result) == 0:
        maxRetries -= 1
        print(f"Get alpha {alphaID} IS result failed. {maxRetries} attempts remaining.")
        return get_alpha_result(s, alphaID, maxRetries)

    else:
        is_pnl = is_result["is"]
        return_dict["pnl"] = int(is_pnl["pnl"])
        return_dict["longCount"] = int(is_pnl["longCount"])
        return_dict["shortCount"] = int(is_pnl["shortCount"])
        return_dict["turnover"] = round(float(is_pnl["turnover"]) * 100, 2)
        return_dict["returns"] = round(float(is_pnl["returns"]) * 100, 2)
        return_dict["drawdown"] = round(float(is_pnl["drawdown"]) * 100, 2)
        return_dict["margin"] = round(float(is_pnl["margin"]) * 10000, 2)
        return_dict["sharpe"] = float(is_result["sharpe"])
        return_dict["fitness"] = float(is_result["fitness"])
        return_dict["alpha"] = is_result["regular"]["code"]
        return return_dict


def regular_simulate(s: requests.Session, alpha: str, region: str, universe: str, delay: int, decay: int, neutralization: str, truncation: float, pasteurization="ON", testPeriod="P0Y0M", unitHandling="VERIFY", nanHandling="ON", maxTrade="OFF", maxRetries=3) -> str | None:
    """
    Simulate an alpha, check its completion status every 10 ~ 30 seconds.
    :param s: REQUIRED. Your ``requests.Session`` object
    :param alpha: REQUIRED. e.g. `rank(-close)`
    :param region: REQUIRED. e.g. `ASI`
    :param universe: REQUIRED. e.g. `TOP3000`
    :param delay: REQUIRED. Either 0 or 1
    :param decay: REQUIRED. Non-negative integers
    :param neutralization: REQUIRED. e.g. `CROWDING`
    :param truncation: REQUIRED. A float between 0 and 1
    :param pasteurization: Either `"ON"` or `"OFF"`
    :param testPeriod: In format of `PaYbM`, where `a` = number of test years, `b` = number of test months. Default to `P0Y0M` (Zero test period)
    :param unitHandling: Must be `VERIFY` at this stage
    :param nanHandling: Either `"ON"` or `"OFF"`
    :param maxTrade: Either `"ON"` or `"OFF"`
    :param maxRetries: When provided, stop retry getting alpha result after this many retries. Default to 3
    :return: A string `alphaID` which can be used to get the simulation results.
    """

    if maxRetries <= 0:
        warnings.warn(f"{maxRetries} attempts exceeded. Stop simulating this alpha.")
        return None

    sim_data = {"type": "REGULAR", "settings": {"instrumentType": "EQUITY", "region": region, "universe": universe, "delay": delay, "decay": decay, "neutralization": neutralization, "truncation": truncation, "pasteurization": pasteurization, "testPeriod": testPeriod, "unitHandling": unitHandling, "nanHandling": nanHandling, "maxTrade": maxTrade, "language": "FASTEXPR", "visualization": False}, "regular": alpha}

    sim_resp = s.post(simulation_url, json=sim_data)
    progress_url = sim_resp.headers["Location"]

    while True:
        sim_progress = s.get(progress_url)
        try:
            alphaID = sim_progress.json()["alpha"]
            break
        except KeyError:
            time.sleep(10 + 20 * random.random())
        except requests.exceptions.ConnectionError:  # Alpha simulation is stopped already
            maxRetries -= 1
            print(f"Alpha simulation failed. {maxRetries} attempts remaining.")
            return regular_simulate(s, alpha, region, universe, delay, decay, neutralization, truncation, pasteurization, testPeriod, unitHandling, nanHandling, maxTrade, maxRetries)

    return alphaID


def multi_simulate(s: requests.Session, alphas: list[str], region: str, universe: str, delay: int, decay: int, neutralization: str, truncation: float, pasteurization="ON", testPeriod="P0Y0M", unitHandling="VERIFY", nanHandling="ON", maxTrade="OFF", maxRetries=3) -> list | None:
    """
    Multi-simulate an alpha, check its completion status every 10 ~ 30 seconds.
    :param s: REQUIRED. Your ``requests.Session`` object
    :param alphas: REQUIRED. List of alpha expressions to simulate
    :param region: REQUIRED. e.g. `ASI`
    :param universe: REQUIRED. e.g. `TOP3000`
    :param delay: REQUIRED. Either 0 or 1
    :param decay: REQUIRED. Non-negative integers
    :param neutralization: REQUIRED. e.g. `CROWDING`
    :param truncation: REQUIRED. A float between 0 and 1
    :param pasteurization: Either `"ON"` or `"OFF"`
    :param testPeriod: In format of `PaYbM`, where `a` = number of test years, `b` = number of test months. Default to `P0Y0M` (Zero test period)
    :param unitHandling: Must be `VERIFY` at this stage
    :param nanHandling: Either `"ON"` or `"OFF"`
    :param maxTrade: Either `"ON"` or `"OFF"`
    :param maxRetries: When provided, stop retry getting alpha result after this many retries. Default to 3
    :return: A list of `alphaID` strings which can be used to get the simulation results
    """

    if maxRetries <= 0:
        warnings.warn(f"{maxRetries} attempts exceeded. Stop simulating this alpha.")
        return None

    data = []
    for alpha in alphas:
        sim_data = {"type": "REGULAR", "settings": {"instrumentType": "EQUITY", "region": region, "universe": universe, "delay": delay, "decay": decay, "neutralization": neutralization, "truncation": truncation, "maxTrade": maxTrade, "pasteurization": pasteurization, "testPeriod": testPeriod, "unitHandling": unitHandling, "nanHandling": nanHandling, "language": "FASTEXPR", "visualization": False}, "regular": alpha}
        data.append(sim_data)

    sim_resp = s.post(simulation_url, json=data)
    progress_url = sim_resp.headers["Location"]
    alphaIDs = []

    while True:
        sim_progress = s.get(progress_url)
        try:
            childSimID = sim_progress.json()["children"]
            for sim_id in childSimID:
                result = s.get(simulation_url + f"/{sim_id}")
                alphaID = result.json()["alpha"]
                alphaIDs.append(alphaID)
            break
        except KeyError:
            time.sleep(10 + 20 * random.random())
        except requests.exceptions.ConnectionError:  # Alpha simulation is stopped already
            maxRetries -= 1
            print(f"Alpha simulation failed. {maxRetries} attempts remaining.")
            return multi_simulate(s, alphas, region, universe, delay, decay, neutralization, truncation, pasteurization, testPeriod, unitHandling, nanHandling, maxTrade, maxRetries)

    return alphaIDs


def submit(s: requests.Session, alphaID: str) -> int:
    """
    Submit an alpha.
    :param s: REQUIRED. Your ``requests.Session`` object
    :param alphaID: REQUIRED. Your alphaID to be submitted, e.g. akr9MgER
    :return: Submission status code. 201 indicates success, others all indicate failure.
    """
    url = alpha_url + f"/{alphaID}/submit"
    r = s.post(url)
    status = r.status_code
    return status