from typing import List
from attrs import define, field, Factory
import pandas as pd


@define(slots=False)
class Bookmark:
    """
    {
        "DATA_TYPE": "bookmark",
        "id": "1405823f-0597-4a16-b296-46d4f0ae4b15",
        "name": "genomics dataset1",
        "endpoint_id": "a624df8b-8de2-4a73-a5b1-85b0f4bff2a8",
        "path": "/projects/genomics/dataset1/"
    }
    """
    bookmark_id: str
    name: str
    endpoint_id: str
    path: str

@define(slots=False)
class BookmarkList:
    """ 
        {
        "DATA_TYPE": "bookmark_list",
        "DATA": [
            {
                "DATA_TYPE": "bookmark",
                "id": "1405823f-0597-4a16-b296-46d4f0ae4b15",
                "name": "genomics dataset1",
                "endpoint_id": "a624df8b-8de2-4a73-a5b1-85b0f4bff2a8",
                "path": "/projects/genomics/dataset1/"
            },
            {
                "DATA_TYPE": "bookmark",
                "id": "1405823f-0597-4a16-b296-46d4f0ae4b15",
                "name": "physics dataset7",
                "endpoint_id": "a624df8b-8de2-4a73-a5b1-85b0f4bff2a8",
                "path": "/projects/physics/dataset7/"
            }
        ]
        }
    """
    DATA_TYPE: str
    DATA: List[Bookmark]
    
    def to_dataframe(self) -> pd.DataFrame:
        data_dict = {
            "bookmark_id": [],
            "name": [],
            "endpoint_id": [],
            "path": []
        }

        for bookmark in self.DATA:
            data_dict["bookmark_id"].append(bookmark.bookmark_id)
            data_dict["name"].append(bookmark.name)
            data_dict["endpoint_id"].append(bookmark.endpoint_id)
            data_dict["path"].append(bookmark.path)

        df = pd.DataFrame(data_dict)
        return df
