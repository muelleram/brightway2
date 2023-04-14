"""
Amelie Mueller

Script to import the Simepro Csv file of the LCIs of the Formit/Climwood project
CSV received from Estelle Vial on 31.03.2023

helpful infos:

https://documentation.brightway.dev/en/latest/source/introduction/introduction.html#parameterized-datasets

notebooks: https://github.com/brightway-lca/brightway2/blob/master/notebooks/Parameters%20-%20Excel%20import.ipynb,
https://github.com/brightway-lca/brightway2/blob/master/notebooks/Parameters%20-%20manual%20creation.ipynb

stack overflow issues:
https://github.com/brightway-lca/brightway2-io/issues/122
https://stackoverflow.com/questions/38030993/simapro-dataset-to-ecoinvent-migration-fails-in-brightway2
https://github.com/brightway-lca/brightway2-io/issues/45
https://github.com/brightway-lca/brightway2-io/issues/63
https://github.com/brightway-lca/brightway2-io/issues/79
"""
#External libraries
import brightway2 as bw
import pandas as pd
import bw2io
import functools
import bw2data as bd
from bw2io.strategies import link_iterable_by_fields
from bw2data.database import Database

#internal functions
from Raphael_jolivet_matching_script_annotatedAM import match_simapro_eco
from Formit_biosphere_matching import create_biosphere_migration_schema

# select project
bw.projects.set_current("Formit_LCIs_v4")
databases = bd.databases
print("Databases in the current project:")
for db in databases:
    print(db)

#Import CSV
#SimaproCSVIMporter already uses SimaproCSVExtractor
sp=bw.SimaProCSVImporter(r"C:\Users\MULLERA\OneDrive - VITO\Documents\05_data\Formit LCIs\Substitution_v20230331.csv", "Formit_LCIs")
print("\nAfter import:")
sp.statistics()

#Apply various different strategies
print("\nAfter apply strategies:") #unlinked exchanges: 17176 -> 2288
sp.apply_strategies()
sp.statistics()

print("\nMatch ecoinvent:") #unlinked exchanges: 2288 -> 2271
sp.match_database("cutoff38", relink=True, fields=['name', 'unit', 'location']) #, "reference product"]) #adding reference product doesn't help; does link_iterable_by_fields under the hood
sp.statistics()

print("\nMatch biosphere:") #unlinked exchanges: 2271 -> 2271 (doesn't match anything)
sp.match_database("biosphere3",relink=True, kind='biosphere') #does link_iterable_by_fields under the hood
sp.statistics()

print("\nMigrate ecoinvent 3.4:") #unlinked exchanges: 2271 -> 1162
sp.migrate("simapro-ecoinvent-3.4")
sp.apply_strategy(functools.partial(
        link_iterable_by_fields,
        other=bw.Database("cutoff38"),
        kind="technosphere",
        fields=["reference product", "name", "unit", "location"]))
sp.statistics()


## Match remaining technopshere ##
#get matching data between simapro and ecoinvent for remaining, easy to migrate unlinked exchanges, using modified code from Raphael
ei_db=bw.Database("cutoff38")
matching_data_technosphere = match_simapro_eco(sp, ei_db)

# initialize empty migration data dictionary
migration_data_formit_techn = {
    'fields': ['name'],
    'data': []}
#fill migration data dictionary with values from matching data
for key, value in matching_data_technosphere.items():
    migration_data_formit_techn['data'].append(
        (
            (key,), #needs to be a tuple
            {
        'name': value['name'],
        'reference product': value['reference product'],
        'location': value['location'],
        'unit': value['unit']}
        )
        )

#write migration strategy
bw2io.Migration("migration_formit_all_easy_techn_flows").write(migration_data_formit_techn, description = "migration of easy to migrate unlinked exchanges (of the structure reference product {location} | activity name | Cutoff U ")
#apply migration strategy
sp.migrate("migration_formit_all_easy_techn_flows")
sp.apply_strategy(functools.partial(
        link_iterable_by_fields,
        other=bw.Database("cutoff38"),
        kind="technosphere",
        fields=["reference product", "name", "unit", "location"] #
))
print("\nAfter custom migration of unlinked technosphere exchanges of the structure reference product {location} | activity name | Cutoff U") #1162 ->
sp.statistics()

#%%

## Match remaining biosphere ##
#match biosphere flows with custom function:
migration_data_formit_bio=create_biosphere_migration_schema()
#TODO think about remaining unmatched biosphere flows: which can be still matched & for those that can't: are they relevant from a CC perspective?

#write custom biosphere migration strategy
bw2io.Migration("migration_formit_biosphere").write(migration_data_formit_bio, description = "migration of (part of) the biosphere flows")
#apply strategy
sp.migrate("migration_formit_biosphere_test")
sp.apply_strategy(functools.partial(
        link_iterable_by_fields,
        other=bw.Database("biosphere3"),
        kind="biosphere",
        fields=['name', 'categories', 'unit'] 
))
print("\nAfter custom migration for unlinked simapro biosphere exchanges")
sp.statistics()    


# write all still unlinked processes to excel
sp.write_excel(only_unlinked=True)    
#dropping still unlinked processes for now
sp.drop_unlinked(i_am_reckless=True) #TODO in the end, nothing major must be dropped here!
#write database to brightway
sp.write_database()

#Import database parameters
#create a list of dictionaries for the parameters, based on the values of sp.global_parameters
formit_db_params = []

for key in sp.global_parameters.keys():
    param_dict = {
        'name': key,
        'amount': sp.global_parameters[key].get("amount", 0),
        'data': {
            "comment": sp.global_parameters[key].get("comment", ""),
            "loc": sp.global_parameters[key].get("loc", 0),
            "uncertainty type": sp.global_parameters[key].get("uncertainty type", 0) #TODO think if other datatype (.e.g. NONE) should be written if there is none
        }
    }
    formit_db_params.append(param_dict)

#write parameters to Brightway
bw.parameters.new_database_parameters(formit_db_params, "Formit_LCIs")

#Document the custom matching of biosphere and technosphere flows for transparency
#%%

excel_writer = pd.ExcelWriter("custom_matched_flows_logbook.xlsx")

df_dict = pd.DataFrame.from_dict(matching_data_technosphere, orient="index", columns=["activity", "reference product", "location", "unit"])
df_dict.to_excel(excel_writer, sheet_name="Matched techn exch")

#TODO add another tab to document matching of biosphere exchanges

excel_writer.save()