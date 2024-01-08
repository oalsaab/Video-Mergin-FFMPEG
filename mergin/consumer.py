from asyncio import PriorityQueue

from producer import Work


async def consumer(queue: PriorityQueue):
    while not queue.empty():
        item: Work = await queue.get()

        # Write contents to file or list

        print(f"Consuming {item.file} --> {item.creation}")

        queue.task_done()
