import re
with open('olist_dw_erd.drawio', encoding='utf-8') as f:
    content = f.read()

table_blocks = re.split(r'(?=value="(?:dim_|fact_)\w+" vertex)', content)
for block in table_blocks:
    tname = re.search(r'value="(dim_\w+|fact_\w+)" vertex', block)
    if tname:
        types = ['INTEGER','SERIAL','VARCHAR','SMALLINT','BOOLEAN','DECIMAL','DATE','TEXT','FLOAT','NUMERIC']
        pattern = 'value="([^"<>]+(?:' + '|'.join(types) + ')[^"<>]*)"'
        fields2 = re.findall(pattern, block)
        print(f'\n=== {tname.group(1)} ===')
        for f2 in fields2:
            print(' ', f2)
