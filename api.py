from sds_import import (
    FastAPI, HTTPException, Depends,
    BaseModel, Dict, List, Any, Optional,
    json
)
from sqlalchemy.ext.asyncio import AsyncSession
from db import get_async_session
from search import ProductSearch

app = FastAPI(
    title="LDB - Product Search API",
    description="API for structured product search and information retrieval",
    version="2.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Root endpoint
@app.get("/", 
         summary="API Root",
         description="Get basic information about the API")
def read_root():
    """
    Root endpoint for the LDB API.

    Returns basic information about the API and links to documentation.
    """
    return {
        "name": "LDB - Product Search API",
        "version": "2.0",
        "description": "API for structured product search and information retrieval",
        "documentation": {
            "Swagger UI": "/docs",
            "ReDoc": "/redoc"
        },
        "endpoints": {
            "structured_search": "/search/structured",
            "structured_search_v2": "/search/structured_v2"
        }
    }

# Dependency to get the database session
# Using the async session from db.py
get_db = get_async_session

# Pydantic models for request validation
class CharacteristicsModel(BaseModel):
    """
    Model for product characteristics.

    This is a dynamic model that allows any characteristic name as a field
    with a list of possible values.

    Example:
    ```json
    {
        "Длина": ["3м", "2м"],
        "Цвет": ["синий"]
    }
    ```
    """
    class Config:
        extra = "allow"  # Allow extra fields

class IncludeExcludeModel(BaseModel):
    """
    Model for inclusion or exclusion criteria.

    Attributes:
        articles: List of article numbers to include/exclude
        keys: List of keywords to include/exclude
        characteristics: Dictionary of characteristics to include/exclude
    """
    articles: Optional[List[str]] = []
    keys: Optional[List[str]] = []
    characteristics: Optional[CharacteristicsModel] = {}

class SearchCriteriaModel(BaseModel):
    """
    Model for structured search criteria.

    Attributes:
        include: Criteria for including products in search results
        exclude: Criteria for excluding products from search results
    """
    include: IncludeExcludeModel
    exclude: Optional[IncludeExcludeModel] = None

@app.post("/search/structured", 
         summary="Structured Product Search",
         description="Perform a structured search for products based on inclusion and exclusion criteria")
async def structured_search(search_criteria: SearchCriteriaModel, db: AsyncSession = Depends(get_db)):
    """
    Perform a structured search based on the provided criteria.

    This endpoint allows searching for products using a combination of:
    - Article numbers
    - Keywords (in product name, class, or purpose)
    - Specific characteristics and their values

    The search can include both inclusion criteria (products that match) and
    exclusion criteria (products to filter out from the results).

    ### Example Request:
    ```json
    {
      "include": {
        "articles": ["01-0023", "KR-91-0840"],
        "keys": ["Кабель силовой", "Патч-корд"],
        "characteristics": {
          "Длина": ["3м", "2м"],
          "Цвет": ["синий"]
        }
      },
      "exclude": {
        "articles": [],
        "keys": [],
        "characteristics": {}
      }
    }
    ```

    ### Returns:
    A JSON object with:
    - List of matching article numbers
    - Clarifications for further filtering
    - Metadata about the search operation

    ### Example Response:
    ```json
    {
      "articles": ["01-0023", "..."],
      "clarifications": {
        "classes": ["Кабель связи акустический", "..."],
        "groups": ["Патч-корды", "..."],
        "characteristics": {
          "Длина": ["3м", "2м", "..."],
          "Цвет": ["синий", "красный", "..."]
        }
      },
      "metadata": {
        "start_time": "2023-05-20 12:34:56",
        "end_time": "2023-05-20 12:34:57",
        "duration_seconds": 1.23
      }
    }
    ```
    """
    try:
        # Convert Pydantic model to dict
        criteria_dict = search_criteria.dict(exclude_none=True)

        # Initialize the search engine
        search = ProductSearch(db)

        # Perform the search
        results = await search.structured_search(criteria_dict)

        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

@app.post("/search/structured_v2",
         summary="Enhanced Structured Product Search",
         description="Perform a structured search with improved filtering logic")
async def structured_search_v2(search_criteria: SearchCriteriaModel, db: AsyncSession = Depends(get_db)):
    """
    Perform a structured search with enhanced logic based on the provided criteria.

    This endpoint uses an improved search algorithm compared to /search/structured:
    1. First collects all articles matching "articles" and "keys" criteria
    2. Then filters those results based on "characteristics" criteria

    This two-step approach provides better results when searching for products
    with specific characteristics within a category or keyword group.

    ### Example Request:
    ```json
    {
      "include": {
        "articles": ["01-0023", "KR-91-0840"],
        "keys": ["Кабель силовой", "Патч-корд"],
        "characteristics": {
          "Длина": ["3м", "2м"],
          "Цвет": ["синий"]
        }
      },
      "exclude": {
        "articles": [],
        "keys": [],
        "characteristics": {}
      }
    }
    ```

    ### Returns:
    A JSON object with:
    - List of matching article numbers
    - Clarifications for further filtering
    - Metadata about the search operation

    ### Example Response:
    ```json
    {
      "articles": ["01-0023", "..."],
      "clarifications": {
        "classes": ["Кабель связи акустический", "..."],
        "groups": ["Патч-корды", "..."],
        "characteristics": {
          "Длина": ["3м", "2м", "..."],
          "Цвет": ["синий", "красный", "..."]
        }
      },
      "metadata": {
        "start_time": "2023-05-20 12:34:56",
        "end_time": "2023-05-20 12:34:57",
        "duration_seconds": 1.23
      }
    }
    ```
    """
    try:
        # Convert Pydantic model to dict
        criteria_dict = search_criteria.dict(exclude_none=True)

        # Initialize the search engine
        search = ProductSearch(db)

        # Perform the search using the v2 algorithm
        results = await search.structured_search_v2(criteria_dict)

        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

if __name__ == "__main__":
    import os
    import uvicorn
    from dotenv import load_dotenv

    # Загрузка переменных окружения из файла .env
    load_dotenv()

    # Получение порта из переменных окружения
    port = int(os.getenv("API_PORT", 9898))

    uvicorn.run(app, host="0.0.0.0", port=port)
