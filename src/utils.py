def compare_arrays(array1, array2):
    a1 = set(array1)
    a2 = set(array2)
    a1_only = a1.difference(a2)
    a2_only = a2.difference(a1)
    return list(a1_only), list(a2_only)
def is_eq(array1, array2):
    a1_only, a2_only = compare_arrays(array1, array2)
    return (len(a1_only) == 0) and (len(a2_only) == 0)