import asyncio
import json

from aiohttp import ClientSession
from more_itertools import chunked

from models import DbSession, Character, init_orm, close_orm

CHUNK_LEN = 5
BASE_URL = 'https://swapi.py4e.com/api/people/'


async def make_string(http_session: ClientSession, field: list | str) -> str:
    result = ""
    if isinstance(field, str):
        response = await http_session.get(field)
        resp_json = await response.json()
        result = resp_json.get('name') if resp_json.get('name') else resp_json.get('title')
    elif isinstance(field, list):
        names = []
        for url in field:
            response = await http_session.get(url)
            resp_json = await response.json()
            name = resp_json.get('name') if resp_json.get('name') else resp_json.get('title')
            names.append(name)
        result = ', '.join(names) if names else ""
    return result


async def get_character(http_session: ClientSession, character_id: int) -> json:
    response = await http_session.get(f'{BASE_URL}{character_id}/')
    json_data = await response.json()
    return json_data


async def insert_characters(http_session: ClientSession, json_list: list) -> None:
    async with DbSession() as session:
        objects = []
        for json_data in json_list:
            data = {
                'birth_year': json_data.get('birth_year', ""),
                'eye_color': json_data.get('eye_color', ""),
                'films': await make_string(http_session, json_data.get('films')),
                'gender': json_data.get('gender', ""),
                'hair_color': json_data.get('hair_color', ""),
                'height': json_data.get('height', ""),
                'homeworld': await make_string(http_session, json_data.get('homeworld')),
                'mass': json_data.get('mass', ""),
                'name': json_data.get('name', ""),
                'skin_color': json_data.get('skin_color', ""),
                'species': await make_string(http_session, json_data.get('species')),
                'starships': await make_string(http_session, json_data.get('starships')),
                'vehicles': await make_string(http_session, json_data.get('vehicles')),
            }
            objects.append(Character(**data))
        session.add_all(objects)
        await session.commit()


async def main() -> None:
    await init_orm()
    async with ClientSession() as http_session:
        response = await http_session.get(BASE_URL)
        resp_json = await response.json()
        count = resp_json.get('count')
        print(count)
        for chunk in chunked(range(1, count + 1), CHUNK_LEN):
            coros = [get_character(http_session, i) for i in chunk]
            json_list = await asyncio.gather(*coros)
            asyncio.create_task(insert_characters(http_session, json_list))
        tasks = asyncio.all_tasks()
        tasks.remove(asyncio.current_task())
        await asyncio.gather(*tasks)
        await close_orm()


asyncio.run(main())
