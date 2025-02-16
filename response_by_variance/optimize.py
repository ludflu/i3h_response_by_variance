from pulp import LpMaximize, LpProblem, LpVariable, lpSum
import polars as pl
import numpy as np
import time


def convert_to_2d_array(df: pl.DataFrame) -> np.ndarray:
    columns = df.columns
    arr = df.to_numpy()
    xlen, ylen = arr.shape
    for i in range(xlen):
        for j in range(ylen):
            print(f"{columns[i]} x {columns[j]}: {arr[i, j]}")
    # values = [arr.to_list() for arr in df.get_column("values")]
    # arr = np.array(values)
    breakpoint()
    return arr


def find_best_combos(cdf: pl.DataFrame, response_and_variance: pl.DataFrame):
    combo_names = cdf.columns
    correlation_matrix = cdf.to_numpy()
    xlen, ylen = correlation_matrix.shape

    combo_names = cdf.columns
    combo_indices = range(xlen)
    combo_dict = dict(zip(combo_names, combo_indices))

    variance_dict: dict[str, float] = {}
    response_dict: dict[str, float] = {}

    for pop, reagent, _condition, median, variance in response_and_variance.iter_rows():
        group_key = f"{pop},{reagent},{_condition}"
        indx = combo_dict[group_key]
        variance_dict[indx] = variance
        response_dict[indx] = median

    print("setting up the problem...\n")
    # breakpoint()
    # Example Data
    # objects = [1, 2, 3, 4]  # Object IDs
    # values = {1: 10, 2: 15, 3: 9, 4: 5}  # median response of each object
    # weights = {1: 4, 2: 8, 3: 5, 4: 3}  # variance of each object
    # repulsion = {(1, 3): 0.9, (2, 4): 0.3, (3, 4): 0.6}  # Repulsion values (symmetric)

    # Parameters for balancing objectives
    alpha = 1  # Weight for value maximization
    beta = 0.5  # Weight for weight maximization
    gamma = 1  # Weight for repulsion effect

    # Decision variables: x[i] = 1 if object i is selected, 0 otherwise
    x = {i: LpVariable(f"x_{i}", cat="Binary") for i in combo_indices}

    # Auxiliary variables for pairwise repulsion terms
    y = {
        (i, j): LpVariable(f"y_{i}_{j}", cat="Binary")
        for i in combo_indices
        for j in combo_indices
        if i < j
    }

    # Define the problem
    prob = LpProblem("Maximize_Objective", LpMaximize)

    # Objective function (ignoring attraction, keeping repulsion)
    prob += (
        alpha * lpSum(variance_dict[i] * x[i] for i in combo_indices)
        + beta * lpSum(response_dict[i] * x[i] for i in combo_indices)
        -
        # note we subtract the repulsion in order to minimize it
        gamma * lpSum(abs(correlation_matrix[i, j]) * y[i, j] for (i, j) in y)
    )

    # Constraints to enforce y[i, j] = x[i] * x[j]
    for i, j in y:
        prob += y[i, j] <= x[i]  # y_ij can only be 1 if x_i is 1
        prob += y[i, j] <= x[j]  # y_ij can only be 1 if x_j is 1
        prob += y[i, j] >= x[i] + x[j] - 1  # y_ij = 1 iff both x_i and x_j are 1

    # Optional Constraints:
    # max_weight = 15  # Example max weight limit
    # prob += lpSum(weights[i] * x[i] for i in objects) <= max_weight

    min_selection = 1  # Minimum number of objects selected
    max_selection = 3  # Maximum number of objects selected
    prob += lpSum(x[i] for i in combo_indices) >= min_selection
    prob += lpSum(x[i] for i in combo_indices) <= max_selection

    start_time = time.time()
    print("solving... (this may take a while)\n")
    print(f"start time: {start_time}")
    # Solve the problem and time it
    prob.solve()
    elapsed_time = time.time() - start_time
    print(f"done solving (took {elapsed_time:.2f} seconds)\n")
    # Output results
    selected_objects = [combo_names[i] for i in combo_indices if x[i].value() == 1]
    print("Selected objects:", selected_objects)
    print("Objective value:", prob.objective.value())
