import numpy as np
import pandas as pd

def simulate(n, peers=1, seeds=0):
    infected = np.array([False] * n)
    infected[0] = True
    iteration = 1

    while not all(infected):
        for i in range(n):
            idxs = np.random.choice(np.arange(n), peers)

            if seeds > 0 and np.random.rand() <= 0.3:
                idxs2 = np.random.choice(np.arange(seeds), 1)
                idxs = np.concatenate((idxs, idxs2))

            infected[idxs] |= infected[i]
            infected[i] |= any(infected[idxs])

        iteration += 1

    return iteration


data = []
for n_servers in range(100, 5001, 200):
    print(n_servers)
    for i in range(30):
        its = simulate(n_servers, peers=1, seeds=0)
        data.append(dict(n_servers=n_servers, seeds=False, rounds=its))

        its = simulate(n_servers, peers=1, seeds=3)
        data.append(dict(n_servers=n_servers, seeds=True, rounds=its))

df = pd.DataFrame.from_records(data)
df.to_csv('data.csv')
