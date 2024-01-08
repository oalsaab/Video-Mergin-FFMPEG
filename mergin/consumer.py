from asyncio import PriorityQueue

from producer import Work


async def consumer(queue: PriorityQueue) -> list[str]:
    ordered = []

    while not queue.empty():
        item: Work = await queue.get()

        ordered.append(f"file '{item.file.name}'")
        queue.task_done()

        print(f"Consuming {item.file.name} --> {item.creation}")

    return ordered
