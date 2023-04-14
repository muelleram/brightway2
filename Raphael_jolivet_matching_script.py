from collections import defaultdict

GLO_LOCS = ["GLO", "RoW"]


def canonic(val):
    """Canonicalize string : split into uniq words and sort them alphabetically"""
    val = val.lower().replace(",", " ")
    words = set(word for word in val.split(" ") if word != "")
    return " ".join(sorted(words))


def same_loc(loc1, loc2):
    return loc1 == loc2 or (loc1 in GLO_LOCS and loc2 in GLO_LOCS)


def format_acts(acts):
    """ Pretty print """
    return "\n".join("name: '%s', unit:%s, loc:%s, ref:''%s(%s)'" % (
        act["name"],
        act["unit"],
        act["location"],
        act["reference product"],
        canonic(act["reference product"])) for act in acts)


def select_act(acts, ex, loc, ref_name, sima_name):
    """ Find tthe match between several candidates : filter by loc and ref product"""

    unit = ex["unit"]
    canon_ref_name = canonic(ref_name)

    matches = [act for act in acts
               if same_loc(loc, act["location"])
               and canonic(act["reference product"]) == canon_ref_name]

    if len(matches) == 1:

        res = matches[0]

        if res["unit"] != unit:
            print("Mismatched unit for %s : %s != %s" % (ref_name, unit, res["unit"]))

        return res

    search = "sima=%s, ref_name=%s, loc=%s, unit=%s" % (sima_name, ref_name, loc, unit)

    if len(matches) > 1:
        print("\nToo much matches for (%s) :\n%s" % (search, format_acts(matches)))

    if len(matches) == 0:
        print("\nNot found (%s) in :\n%s" % (search, format_acts(acts)))


def process_ex(ex, ei_by_name):
    """Process a single exchange """
    sima_name = ex["name"]
    if "|" not in sima_name:  # not all unlinked exchanges have the pattern that Rapahael filters by, here I skip the ones that don't fit his structure
        print(" %s does not fit Raphaels algorithm, will be skipped" % ex)
        return

    else:
        name_loc, suffix, _ = sima_name.split("|")
        name_loc = name_loc.strip("}")
        ref_name, loc = name_loc.split("{")

        for test in [ref_name + " " + suffix, ref_name, suffix]:
            canon_test = canonic(test)
            if canon_test in ei_by_name:
                candidates = ei_by_name[canon_test]
                act = select_act(candidates, ex, loc, ref_name, sima_name)
                return

    print("No match found for %s" % ex)
    return


#  -- Main process
def match_simapro_eco(simapro_importer, ei_db) :
    # Loop on unlinked exchanges
    list_irregular_exc = []
    for ex in simapro_importer.unlinked:
        if "|" not in sima_name: #not all unlinked exchanges have the pattern that Rapahael filters by, here I skip the ones that don't fit his structure
            list_irregular_exc.append(sima_name)
            return
        else:
        # Build a index of activity per canonic name
            ei_by_name = defaultdict(set)
            for act in ei_db:
                ei_by_name[canonic(act["name"])].add(act)

            if ex["type"] == "technosphere":
                process_ex(ex, ei_by_name)