import pandas as pd
import numpy as np
import bitstring
import json

normal_type = 'normal.'

with open('params.json') as data_file:    
    params = json.load(data_file)

df_0 = pd.read_csv(params['filename'])

# штатные ситуации
normal_1 = df_0[ df_0['type'] == normal_type ]
normal = normal_1.reindex(np.random.permutation(normal_1.index))

# нештатные ситуации
attack_1 = df_0[ df_0['type'] == params['attack_type'] ]
attack = attack_1.reindex(np.random.permutation(attack_1.index))

# контрольная выборка
test_1 = normal_1.reindex(np.random.permutation(normal_1.index)).head(params['normal_count'])
test_2 = attack_1.reindex(np.random.permutation(attack_1.index)).head(params['attack_count'])
test = test_1.append(test_2)
test = test.reindex(np.random.permutation(test.index))

def prepare_row(row):
    if row['type'] == normal_type:
        row['type'] = 0
    else:
        row['type'] = 1
    return row

# код Грея
def g(n):
	return n ^ (n >> 1);

def process_float_to_binary(row, unique, property):
    order_number = unique.index(row[property])
    t = bitstring.BitArray(uint = int(g(order_number)), length = len(unique).bit_length())
    row[property] = str( t.bin )
    return row

def prepare_property(df, property):
    unique = df_0[property].unique().tolist()
    df_new = df.apply( lambda row: process_float_to_binary(row, unique, property), axis = 1 )
    return df_new

def write_dataset(prefix, df, columns = ['bin']):
    protocol_unique = df_0['protocol_type'].unique().tolist()
    df_2 = df.apply( lambda row: process_float_to_binary(row, protocol_unique, 'protocol_type'), axis = 1 ) 
    
    df_2 = prepare_property(df_2, 'duration')
    df_2 = prepare_property(df_2, 'src_bytes')
    df_2 = prepare_property(df_2, 'dst_bytes')
    df_2 = prepare_property(df_2, 'urgent')
    df_2 = prepare_property(df_2, 'count')
    df_2 = prepare_property(df_2, 'srv_count')
    df_2 = prepare_property(df_2, 'dst_host_count')
    df_2 = prepare_property(df_2, 'dst_host_srv_count')

    df_2['bin'] = df_2['protocol_type'] + df_2['duration'] + df_2['src_bytes'] + df_2['dst_bytes'] + df_2['urgent'] + df_2['count'] + df_2['srv_count'] + df_2['dst_host_count'] + df_2['dst_host_srv_count']
    df_2.to_csv("datasets/" + prefix + str(params['dataset']) + '.csv', index = False, columns = columns)

write_dataset("normal", normal.head(params['normal_count']))
write_dataset("attack", attack.head(params['attack_count']))

test = test.apply( lambda row: prepare_row(row), axis = 1 )
write_dataset("test", test, ['bin', 'type'])

params['dataset'] += 1
with open('params.json', 'w') as outfile:
    json.dump(params, outfile) 

def column_describe(col_name, f):
    print(col_name) 
    #print( f[col_name].unique() )
    print( f[col_name].min() )
    print( f[col_name].max() )
    print( len( f[col_name].unique() ))

#column_describe('duration', df)
#column_describe('protocol_type', df)
#column_describe('service', df)
#column_describe('src_bytes', df)
#column_describe('dst_bytes', df)
#column_describe('urgent', df)
#column_describe('count', df)
#column_describe('srv_count', df)
#column_describe('same_srv_rate', df)
#column_describe('dst_host_count', df)
#column_describe('dst_host_srv_count', df)
#column_describe('dst_host_same_srv_rate', df)
#column_describe('dst_host_same_src_port_rate', df)
