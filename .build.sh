IMAGE_NAME=tybalex/opni-parsing:dev
docker build . -t $IMAGE_NAME

docker push $IMAGE_NAME
