from read_data import get_final_data
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 200)
pd.set_option('expand_frame_repr', False)


def main():
    df = get_final_data()
    while True:
        inp1 = input("Give desired periods separated with commas: ")
        inp2 = input('Give desired minimum credits: ')

        if inp1 == "" or inp2 == "":
            break
        if inp2.isdigit() == False:
            print("Incorrect input for course credits\n")
            continue

        cols = inp1.split(',')
        cols = [col for col in cols if col in df.columns]
        ind = df.loc[:, cols].any(axis="columns")
        results = df[ind]
        results = results.loc[results["credits_min"] >= int(inp2), :]

        print("\n")
        print("Found {n} courses for periods {s}".format(
            n=len(results), s=cols))
        print("\n")
        print(results.iloc[:, :4].sort_values(
            by=["id", "credits_min"], ascending=False))
        print("\n")


if __name__ == '__main__':
    main()
