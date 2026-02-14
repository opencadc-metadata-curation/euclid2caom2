# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2026.                            (c) 2026.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

from euclid2caom2 import EUCLIDName, EUCLID_DR1_Name


def test_is_valid():
    assert EUCLIDName(
        source_names=['EUC_MER_BGSUB-MOSAIC-VIS_TILE102070858-5ED2D5_20241105T125727.727353Z_00.00.fits']
    ).is_valid()


def test_storage_name_locations(test_config):
    # test that location patterns can be handled by the constructor
    test_f_name = f'EUC_MER_BGSUB-MOSAIC-VIS_TILE102070858-5ED2D5_20241105T125727.727353Z_00.00.fits'
    # uris refer to the namespace in SI, and not the collection
    test_uri = f'{test_config.scheme}:{test_config.namespace}/{test_f_name}'
    for index, entry in enumerate(
        [
            test_f_name,
            test_uri,
            f'https://localhost:8020/{test_f_name}',
            f'vos:goliaths/test/{test_f_name}',
            f'/tmp/{test_f_name}',
        ]
    ):
        test_subject = EUCLIDName(source_names=[entry])
        assert test_subject.file_id == test_f_name.replace('.fits', '').replace(
            '.header', ''
        ), f'wrong file id {index}'
        assert test_subject.file_uri == test_uri, f'wrong uri {index}'
        assert test_subject.source_names == [entry], f'wrong source names {index}'
        assert test_subject.destination_uris == [test_uri], f'wrong uris {index} {test_subject}'


def test_storage_name(test_config):
    # test that obs_id, product_id setting is handled by the ctor
    entries = {
        'TILE102070858': {
            'esa:EUCLID/EUC_MER_BGSUB-MOSAIC-VIS_TILE102070858-5ED2D5_20241105T125727.727353Z_00.00.fits': 'VIS',
            'esa:EUCLID/EUC_MER_BGMOD-VIS_TILE102070858-F79595_20241105T125727.727179Z_00.00.fits': 'VIS',
            'esa:EUCLID/EUC_MER_GRID-PSF-VIS_TILE102070858-7333BC_20241104T161703.183167Z_00.00.fits': 'VIS',
            'esa:EUCLID/EUC_MER_MOSAIC-VIS-RMS_TILE102070858-BB87CE_20241104T161703.183124Z_00.00.fits': 'VIS',
            'esa:EUCLID/EUC_MER_MOSAIC-VIS-FLAG_TILE102070858-B260B9_20241104T161703.183145Z_00.00.fits': 'VIS',
            'esa:EUCLID/EUC_MER_CATALOG-PSF-VIS_TILE102070858-F69E9E_20241105T134129.112687Z_00.00.fits': 'VIS',
            'esa:EUCLID/EUC_MER_FINAL-CAT_TILE102070858-FCBD03_20241106T175237.497132Z_00.00.fits': 'CAT',
            'esa:EUCLID/EUC_MER_FINAL-CUTOUTS-CAT_TILE102070858-755293_20241106T105450.172109Z_00.00.fits': 'CUTOUTS_CAT',
            'esa:EUCLID/EUC_MER_FINAL-MORPH-CAT_TILE102070858-46295E_20241106T175236.702482Z_00.00.fits': 'MORPH_CAT',
        },
        'TILE102165193': {
            'esa:EUCLID/EUC_MER_BGSUB-MOSAIC-NIR-Y_TILE102165193-683034_20240526T184144.400003Z_00.00.xml': 'Y',
        },
    }

    for tile_id, entry in entries.items():
        for uri, product_id in entry.items():
            test_subject = EUCLIDName(source_names=[uri])
            assert test_subject.obs_id == tile_id, f'obs id {uri}'
            assert test_subject.product_id == f'{tile_id}_{product_id}', f'product id {uri}'


def test_storage_name_dr1(test_config):
    # test that obs_id, product_id setting is handled by the ctor
    entries = [
        'esa:EUCLID_DR1/EUC_MER_BGSUB-MOSAIC-NIR-H_TILE101363214-C9D666_20250730T024715.729229Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_BGSUB-MOSAIC-NIR-J_TILE101363214-2E980D_20250730T024841.170931Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_BGSUB-MOSAIC-NIR-Y_TILE101363214-708EEE_20250730T025218.739390Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_BGSUB-MOSAIC-VIS_TILE101363214-CA6449_20250730T030407.962880Z_00.00.fits',
    ]

    test_config.collection = 'EUCLID_DR1'

    expected_ids = {
        'TILE101363214-NIR-H': 'TILE101363214-NIR-H',
        'TILE101363214-NIR-J': 'TILE101363214-NIR-J',
        'TILE101363214-NIR-Y': 'TILE101363214-NIR-Y',
        'TILE101363214-VIS': 'TILE101363214-VIS',
    }

    for entry in entries:
        test_subject = EUCLID_DR1_Name(source_names=[entry])
        lookup = expected_ids.get(test_subject.obs_id)
        assert lookup is not None, f'wrong obs id {test_subject.obs_id}'
        assert test_subject.product_id == lookup, f'product id {test_subject.product_id}'


def test_storage_name_dr1_a_pointing():
    entries = [
        'esa:EUCLID_DR1/EUC_MER_BGSUB-MOSAIC-VIS_TILE101274905-407419_20251014T091404.360049Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_BGMOD-VIS_TILE101274905-CF0AF0_20251014T091404.359855Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_MOSAIC-VIS-RMS_TILE101274905-7EB2B_20251014T050241.752350Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_BGSUB-MOSAIC-NIR-Y_TILE101274905-F80D3A_20251014T082633.923397Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_BGMOD-NIR-Y_TILE101274905-E11F29_20251014T082633.923202Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_MOSAIC-NIR-Y-RMS_TILE101274905-3AEF44_20251014T051457.568788Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_BGSUB-MOSAIC-NIR-J_TILE101274905-78121E_20251014T071506.028397Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_BGMOD-NIR-J_TILE101274905-339E8D_20251014T071506.028200Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_MOSAIC-NIR-J-RMS_TILE101274905-377543_20251014T044813.228101Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_BGSUB-MOSAIC-NIR-H_TILE101274905-A08ACD_20251014T082407.168820Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_BGMOD-NIR-H_TILE101274905-309F8A_20251014T082407.168627Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_MOSAIC-NIR-H-RMS_TILE101274905-996A89_20251014T045004.082209Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_FINAL-CAT_TILE101274905-C3C0A0_20251014T212916.676848Z_00.00.fits',
        'esa:EUCLID_DR1/EUC_MER_FINAL-MORPH-CAT_TILE101274905-14A4C3_20251014T212915.653487Z_00.00.fits',
    ]

    expected_ids = [
        'TILE101274905-NIR-H',
        'TILE101274905-NIR-J',
        'TILE101274905-NIR-Y',
        'TILE101274905-VIS',
        'TILE101274905-CAT',
    ]
    for entry in entries:
        test_subject = EUCLID_DR1_Name(source_names=[entry])
        assert test_subject.obs_id in expected_ids, f'wrong obs id {test_subject}'
        assert test_subject.product_id in expected_ids, f'wrong product id {test_subject}'
