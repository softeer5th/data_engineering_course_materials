import pandas as pd
data= [[[1,2,3],0,['a','b','c']],[4,[],3],[5,2,['x','y','z']]]
idx = ['row1','row2','row3']
col = ['col1','col2','col3']
df = pd.DataFrame(data = data, index = idx, columns = col)
print(df)
print(df.explode('col3'))