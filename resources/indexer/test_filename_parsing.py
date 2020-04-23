"""Test indexer filename parsing"""

from indexer import (I3FileMetadata, L2FileMetadata, PFDSTFileMetadata, PFFiltFileMetadata,
                     PFRawFileMetadata)


def test_run_number():
    """Run generic filename parsing of run number."""
    r = I3FileMetadata.parse_run_number('Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst')
    assert r == 130484

    r = I3FileMetadata.parse_run_number(
        'Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst')
    assert r == 130567

    r = I3FileMetadata.parse_run_number('Run00125791_GapsTxt.tar')
    assert r == 125791


def _test_filenames(test_filenames, filename_formats):
    for filename, values in test_filenames.items():
        y, r, s, p = I3FileMetadata.parse_year_run_subrun_part(filename_formats, filename)
        print(f"OUTPUTS: {y}, {r}, {s}, {p}")
        assert y == values[0]
        assert r == values[1]
        assert s == values[2]
        assert p == values[3]


def test_L2():
    """Run L2 filename parsing."""
    test_filenames = {
        'Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst': ['2017', 130567, 0, 280],
        'Level2pass2_IC79.2010_data_Run00115975_Subrun00000000_00000055.i3.zst': ['2010', 115975, 0, 55],
        'Level2_PhysicsTrig_PhysicsFiltering_Run00120374_Subrun00000000_00000001.i3': [None, 120374, 0, 1],
        'Level2pass3_PhysicsFiltering_Run00127353_Subrun00000000_00000000.i3.gz': [None, 127353, 0, 0],
        'Level2_IC86.2016_data_Run00129004_Subrun00000316.i3.bz2': ['2016', 129004, 0, 316],
        'Level2_IC86.2012_Test_data_Run00120028_Subrun00000081.i3.bz2': ['2012', 120028, 0, 81],
        'Level2_IC86.2015_24HrTestRuns_data_Run00126291_Subrun00000203.i3.bz2': ['2015', 126291, 0, 203],
        'Level2_IC86.2011_data_Run00119221_Part00000126.i3.bz2': ['2011', 119221, 0, 126],
        'Level2a_IC59_data_Run00115968_Part00000290.i3.gz': ['2009', 115968, 0, 290],
        'MoonEvents_Level2_IC79_data_Run00116082_NewPart00000613.i3.gz': ['2010', 116082, 0, 613],
        'Level2_All_Run00111562_Part00000046.i3.gz': [None, 111562, 0, 46]
    }
    _test_filenames(test_filenames, L2FileMetadata.FILENAME_FORMATS)


def test_PFFilt():
    """Run PFFilt filename parsing."""
    test_filenames = {
        'PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2': [None, 131989, 0, 295],
        'PFFilt_PhysicsTrig_PhysicsFiltering_Run00121503_Subrun00000000_00000314.tar.bz2': [None, 121503, 0, 314],
        'orig.PFFilt_PhysicsFiltering_Run00127080_Subrun00000000_00000244.tar.bz2.orig': [None, 127080, 0, 244],
        'PFFilt_PhysicsTrig_PhysicsFilt_Run00089959_00180.tar.gz': [None, 89959, 0, 180],
        'PFFilt_PhysicsTrig_RandomFilt_Run86885_006.tar.gz': [None, 86885, 0, 6]
    }
    _test_filenames(test_filenames, PFFiltFileMetadata.FILENAME_FORMATS)


def test_PFDST():
    """Run PFDST filename parsing."""
    test_filenames = {
        'ukey_fa818e64-f6d2-4cc1-9b34-e50bfd036bf3_PFDST_PhysicsFiltering_Run00131437_Subrun00000000_00000066.tar.gz': [None, 131437, 0, 66],
        'ukey_42c89a63-e3f7-4c3e-94ae-840eff8bd4fd_PFDST_RandomFiltering_Run00131155_Subrun00000051_00000000.tar.gz': [None, 131155, 51, 0],
        'PFDST_PhysicsFiltering_Run00125790_Subrun00000000_00000064.tar.gz': [None, 125790, 0, 64],
        'PFDST_UW_PhysicsFiltering_Run00125832_Subrun00000000_00000000.tar.gz': [None, 125832, 0, 0],
        'PFDST_RandomFiltering_Run00123917_Subrun00000000_00000000.tar.gz': [None, 123917, 0, 0],
        'PFDST_PhysicsTrig_PhysicsFiltering_Run00121663_Subrun00000000_00000091.tar.gz': [None, 121663, 0, 91],
        'PFDST_TestData_PhysicsFiltering_Run00122158_Subrun00000000_00000014.tar.gz': [None, 122158, 0, 14],
        'PFDST_TestData_RandomFiltering_Run00119375_Subrun00000136_00000000.tar.gz': [None, 119375, 136, 0],
        'PFDST_TestData_Unfiltered_Run00119982_Subrun00000000_000009.tar.gz': [None, 119982, 0, 9]
    }
    _test_filenames(test_filenames, PFDSTFileMetadata.FILENAME_FORMATS)


def test_PFRaw():
    """Run PFRaw filename parsing."""
    test_filenames = {
        'key_31445930_PFRaw_PhysicsFiltering_Run00128000_Subrun00000000_00000156.tar.gz': [None, 128000, 0, 156],
        'ukey_b98a353f-72e8-4d2e-afd7-c41fa5c8d326_PFRaw_PhysicsFiltering_Run00131322_Subrun00000000_00000018.tar.gz': [None, 131322, 0, 18],
        'ukey_05815dd9-2411-468c-9bd5-e99b8f759efd_PFRaw_RandomFiltering_Run00130470_Subrun00000060_00000000.tar.gz': [None, 130470, 60, 0],
        'PFRaw_PhysicsTrig_PhysicsFiltering_Run00114085_Subrun00000000_00000208.tar.gz': [None, 114085, 0, 208],
        'PFRaw_TestData_PhysicsFiltering_Run00114672_Subrun00000000_00000011.tar.gz': [None, 114672, 0, 11],
        'PFRaw_TestData_RandomFiltering_Run00113816_Subrun00000033_00000000.tar.gz': [None, 113816, 33, 0],
        'EvtMonPFRaw_PhysicsTrig_RandomFiltering_Run00106489_Subrun00000000.tar.gz': [None, 106489, 0, 0],
        'DebugData_PFRaw_Run110394_1.tar.gz': [None, 110394, 0, 1]
    }
    _test_filenames(test_filenames, PFRawFileMetadata.FILENAME_FORMATS)
