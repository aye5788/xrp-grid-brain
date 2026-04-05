from src.grid.candidate_builder import build_grid


def generate_latest_grid(df):
    row = df.iloc[-1]

    grid = build_grid(row)

    return {**row.to_dict(), **grid}
