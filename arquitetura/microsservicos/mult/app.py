from fastapi import FastAPI

app = FastAPI()

@app.get("/mult")
def mult(op1: int, op2: int):
    multiplicate = op1 * op2
    return {"result": multiplicate}
