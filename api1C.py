import aiohttp
import os
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Загрузка переменных окружения из файла .env
load_dotenv()

class ApiClient:
    BASE_URL = os.getenv("API_BASE_URL")

    def __init__(self, token: str, timeout: float = 180.0):
        self.token = token
        self.timeout = timeout
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Token {self.token}"
        }
        self.session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self) -> None:
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self.session = aiohttp.ClientSession(
                base_url=self.BASE_URL,
                headers=self.headers,
                timeout=timeout
            )

    @staticmethod
    def _build_params(**kwargs: Any) -> Dict[str, Any]:
        return {k: v for k, v in kwargs.items() if v is not None}

    async def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        await self._ensure_session()
        async with self.session.get(endpoint, params=params) as response:
            if response.status == 201:
                return {"result": []}
            response.raise_for_status()
            return await response.json()

    async def _post(self, endpoint: str, data: Dict[str, Any]) -> Any:
        await self._ensure_session()
        async with self.session.post(endpoint, json=data) as response:
            response.raise_for_status()
            return await response.json()

    async def _put(self, endpoint: str, data: Dict[str, Any]) -> Any:
        await self._ensure_session()
        async with self.session.put(endpoint, json=data) as response:
            response.raise_for_status()
            return await response.json()

    async def _delete(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        await self._ensure_session()
        async with self.session.delete(endpoint, params=params) as response:
            response.raise_for_status()
            return await response.json()

    async def get_categories(self, categoryname: Optional[str] = None, parentid: Optional[int] = None,
                             limit: Optional[int] = None, offset: Optional[int] = None) -> Any:
        params = self._build_params(categoryname=categoryname, parentid=parentid, limit=limit, offset=offset)
        return await self._get("/rexant/hs/api/v1/category", params=params)

    async def get_full_products(self, article: Optional[str] = None, name: Optional[str] = None,
                                brand: Optional[str] = None, country: Optional[str] = None,
                                categoryid: Optional[int] = None, limit: Optional[int] = None,
                                offset: Optional[int] = None) -> Any:
        params = self._build_params(article=article, name=name, brand=brand, country=country,
                                    categoryid=categoryid, limit=limit, offset=offset)
        return await self._get("/rexant/hs/api/v1/product", params=params)

    async def get_short_products(self, article: Optional[str] = None, name: Optional[str] = None,
                                 brand: Optional[str] = None, country: Optional[str] = None,
                                 categoryid: Optional[int] = None, limit: Optional[int] = None,
                                 offset: Optional[int] = None) -> Any:
        params = self._build_params(article=article, name=name, brand=brand, country=country,
                                    categoryid=categoryid, limit=limit, offset=offset)
        return await self._get("/rexant/hs/api/v1/product-short", params=params)

    async def get_analogs(self, productid: Optional[int] = None, productid__article: Optional[str] = None,
                          article: Optional[str] = None, analog_type: Optional[str] = None,
                          limit: Optional[int] = None, offset: Optional[int] = None) -> Any:
        params = self._build_params(
            productid=productid,
            productid__article=productid__article,
            article=article,
            type=analog_type,
            limit=limit,
            offset=offset
        )
        return await self._get("/rexant/hs/api/v1/analog", params=params)

    async def get_barcodes(self, productid: Optional[int] = None, productid__article: Optional[str] = None,
                           article: Optional[str] = None, limit: Optional[int] = None,
                           offset: Optional[int] = None) -> Any:
        params = self._build_params(
            productid=productid,
            productid__article=productid__article,
            article=article,
            limit=limit,
            offset=offset
        )
        return await self._get("/rexant/hs/api/v1/barcode", params=params)

    async def get_etim_classes(self, etimclasskey: Optional[str] = None, rusname: Optional[str] = None,
                               engname: Optional[str] = None, version: Optional[str] = None,
                               limit: Optional[int] = None, offset: Optional[int] = None) -> Any:
        params = self._build_params(etimclasskey=etimclasskey, rusname=rusname,
                                    engname=engname, version=version,
                                    limit=limit, offset=offset)
        return await self._get("/rexant/hs/api/v1/etimclass", params=params)

    async def get_etim_product_attributes(self, productid: Optional[int] = None,
                                          productid__article: Optional[str] = None,
                                          article: Optional[str] = None, etimclasskey: Optional[str] = None,
                                          limit: Optional[int] = None, offset: Optional[int] = None) -> Any:
        params = self._build_params(productid=productid, productid__article=productid__article,
                                    article=article, etimclasskey=etimclasskey,
                                    limit=limit, offset=offset)
        return await self._get("/rexant/hs/api/v1/etimproduct", params=params)

    async def get_certificates(self, productid: Optional[int] = None, productid__article: Optional[str] = None,
                               article: Optional[str] = None,
                               limit: Optional[int] = None, offset: Optional[int] = None) -> Any:
        params = self._build_params(productid=productid, productid__article=productid__article,
                                    article=article,
                                    limit=limit, offset=offset)
        return await self._get("/rexant/hs/api/v1/certificate", params=params)

    async def get_photos(self, productid: Optional[int] = None, productid__article: Optional[str] = None,
                         article: Optional[str] = None, limit: Optional[int] = None,
                         offset: Optional[int] = None) -> Any:
        params = self._build_params(productid=productid, productid__article=productid__article,
                                    article=article, limit=limit, offset=offset)
        return await self._get("/rexant/hs/api/v1/photo", params=params)

    async def get_instructions(self, limit: Optional[int] = None, offset: Optional[int] = None) -> Any:
        params = self._build_params(limit=limit, offset=offset)
        return await self._get("/rexant/hs/api/v1/instructions", params=params)

    async def get_warehouses(self, limit: Optional[int] = None, offset: Optional[int] = None) -> Any:
        params = self._build_params(limit=limit, offset=offset)
        return await self._get("/rexant/hs/api/v1/warehouses", params=params)

    async def get_price_list(self, productid: Optional[int] = None, article: Optional[str] = None,
                            pricetype_name: Optional[str] = None, pricetype_id: Optional[str] = None,
                            limit: Optional[int] = None, offset: Optional[int] = None) -> Any:
        params = self._build_params(
            productid=productid,
            article=article,
            ratenameid__ratename=pricetype_name,
            rateid=pricetype_id,
            limit=limit,
            offset=offset
        )
        return await self._get("/rexant/hs/api/v1/prices", params=params)

    async def get_warehouse_stock(self, productid: Optional[int] = None,
                                 article: Optional[str] = None, storageid: Optional[int] = None,
                                 limit: Optional[int] = None, offset: Optional[int] = None) -> Any:
        params = self._build_params(
            productid=productid,
            article=article,
            storageid=storageid,
            limit=limit,
            offset=offset
        )
        return await self._get("/rexant/hs/api/v1/remain", params=params)

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None
