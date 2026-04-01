import yaml

#load the original file
with open('variables.yaml', 'r') as f:
    raw = yaml.safe_load(f)

#raw['elements'] is a list, loop through it
new_dict = {}
for element in raw['elements']:
    code = element['code']
    new_dict[code] = {
        'name': element['name'],
        'units': element['storedUnitCode'],      
        'description': element.get('description', 'n/a'), 
    }

#write to new file
with open('variables.yaml', 'w') as f:
    yaml.dump(new_dict, f)