import asyncio, httpx, os

from datetime import datetime
from fastapi import FastAPI, Response
from pydantic import BaseModel, validator
from time import time_ns
from typing import Any

os.makedirs('logs', exist_ok=True)

# Task handlers

async def fetch_data(ctx):
	'''Task 1 is to fetch the data from somewhere, so I'm going to make a HTTP request to fetch the data. It could hypothetically fetch the data from a database or elsewhere if were applicable'''
	async with httpx.AsyncClient() as http:
		resp=await http.get(f'https://delilahsthings.ie/toybox/fibbonacci?limit={ctx.parent.result}')
		return [int(a) for a in resp.text.split(', ')]

async def process_data(ctx):
	'''Task 2 is to process the data. As no specific instruction was provided I'm going to multiply all the numbers by themselves'''
	return [a*a for a in ctx.parent.result]

async def store_data(ctx):
	'''Task 3 is to store the data. I'm just going to write it to disk'''
	with ctx.open_file(f"{datetime.now()} results.txt") as output:
		for num in ctx.parent.result:
			output.write(f"{num}\n")

# Models

class Task(BaseModel):
	name: str
	description: str
	handler: str

	@validator('handler')
	def get_handler(cls, v):
		match v:
			case "fetch_data":
				return fetch_data
			case "process_data":
				return process_data
			case "store_data":
				return store_data
			case _:
				raise ValueError(f'Unknown task handler: {v}')

class Condition(BaseModel):
	name: str
	description: str
	source_task: str
	outcome: str
	target_task_success: str
	target_task_failure: str

class Flow(BaseModel):
	id: str
	name: str
	start_task: str
	initial_value: Any
	tasks: list[Task]
	conditions: list[Condition]

class FlowContainer(BaseModel):
	flow: Flow

class WorkUnit:
	def __init__(self, task):
		self.name=task.name
		self.description=task.description
		self.handler=task.handler

		self.on_success=[]
		self.on_failure=[]

class Context:
	def __init__(self, parent=None):
		if parent:
			self.run_id=parent.run_id
			self.log_file=parent.log_file
			self.parent=parent
		else:
			self.run_id=f'{time_ns()}'
			os.mkdir(f'logs/{self.run_id}')
			self.log_file=self.open_file('log')

	def log(self, line):
		self.log_file.write(f'{datetime.now()}    {line}\n')

	def open_file(self, filename):
		return open(f'logs/{self.run_id}/{filename}', 'w+')

# HTTPAPI access

app = FastAPI()

async def execute_tasks(parent_ctx):
	ctx=Context(parent_ctx)

	try:
		ctx.log(f"Begin task {parent_ctx.current_task.name}")
		ctx.result=await parent_ctx.current_task.handler(ctx)
		ctx.log(f"Task {parent_ctx.current_task.name} successful")
		for on_success_callback in parent_ctx.current_task.on_success:
			ctx.current_task=on_success_callback
			await execute_tasks(ctx)
	except Exception as ex:
		ctx.log(f"Task {parent_ctx.current_task.name} failed")
		ctx.log(ex.__str__())
		for on_failure_callback in parent_ctx.current_task.on_failure:
			ctx.current_task=on_failure_callback
			await execute_tasks(ctx)

@app.post("/api/execute_flow", status_code=200)
async def execute_flow(container: FlowContainer, response: Response):
	flow: Flow = container.flow
	task_list = {}

	for task in flow.tasks:
		task_list[task.name]=WorkUnit(task)

	for condition in flow.conditions:
		task = task_list[condition.source_task]
		if condition.target_task_success!='end':
			task.on_success.append(task_list[condition.target_task_success])
		if condition.target_task_failure!='end':
			task.on_failure.append(task_list[condition.target_task_failure])

	ctx=Context()
	ctx.current_task = task_list[flow.start_task]
	ctx.result = flow.initial_value
	asyncio.get_event_loop().create_task(execute_tasks(ctx))

	return {"run_id": ctx.run_id}
