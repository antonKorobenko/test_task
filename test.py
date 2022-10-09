from pandas import DataFrame


def a(a:DataFrame):
    a.loc[len(a.index)] = [1,2]
    

def b():
    df = DataFrame(columns=['first', 'second'])
    a(df)
    print(df)

b()