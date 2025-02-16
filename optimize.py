from pulp import LpMaximize, LpProblem, LpVariable, lpSum

# Example Data
objects = [1, 2, 3, 4]  # Object IDs
values = {1: 10, 2: 15, 3: 9, 4: 5}   # Value of each object
weights = {1: 4, 2: 8, 3: 5, 4: 3}    # Weight of each object
repulsion = { (1,3): 0.9, (2,4): 0.3, (3,4): 0.6 }  # Repulsion values (symmetric)

# Parameters for balancing objectives
alpha = 1  # Weight for value maximization
beta = 0.5 # Weight for weight maximization
gamma = 1  # Weight for repulsion effect

# Decision variables: x[i] = 1 if object i is selected, 0 otherwise
x = {i: LpVariable(f"x_{i}", cat="Binary") for i in objects}

# Auxiliary variables for pairwise repulsion terms
y = {(i, j): LpVariable(f"y_{i}_{j}", cat="Binary") for i in objects for j in objects if i < j and (i, j) in repulsion}

# Define the problem
prob = LpProblem("Maximize_Objective", LpMaximize)

# Objective function (ignoring attraction, keeping repulsion)
prob += (
    alpha * lpSum(values[i] * x[i] for i in objects) +
    beta * lpSum(weights[i] * x[i] for i in objects) - 
    # note we subtract the repulsion in order to minimize it
    gamma * lpSum(repulsion[i, j] * y[i, j] for (i, j) in y)
)

# Constraints to enforce y[i, j] = x[i] * x[j]
for (i, j) in y:
    prob += y[i, j] <= x[i]   # y_ij can only be 1 if x_i is 1
    prob += y[i, j] <= x[j]   # y_ij can only be 1 if x_j is 1
    prob += y[i, j] >= x[i] + x[j] - 1  # y_ij = 1 iff both x_i and x_j are 1

# Optional Constraints:
#max_weight = 15  # Example max weight limit
#prob += lpSum(weights[i] * x[i] for i in objects) <= max_weight

min_selection = 1  # Minimum number of objects selected
max_selection = 3  # Maximum number of objects selected
prob += lpSum(x[i] for i in objects) >= min_selection
prob += lpSum(x[i] for i in objects) <= max_selection

# Solve the problem
prob.solve()

# Output results
selected_objects = [i for i in objects if x[i].value() == 1]
print("Selected objects:", selected_objects)
print("Objective value:", prob.objective.value())


