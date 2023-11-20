from typing import List

import sqlalchemy as _sa


class AlembicSchemaProvider:
    def __init__(self):
        """
        This class is used to register SQLAlchemy metadata objects for alembic
        migrations.

        Alembic accepts multiple metadata objects. So you can register all your
        metadata objects plus the metadata objects of the depeche_db objects.

        see
        https://alembic.sqlalchemy.org/en/latest/autogenerate.html#autogenerating-multiple-metadata-collections
        """
        self._metadata: List[_sa.MetaData] = []

    def register(self, metadata: _sa.MetaData):
        self._metadata.append(metadata)

    def get_metadata(self) -> List[_sa.MetaData]:
        return self._metadata
