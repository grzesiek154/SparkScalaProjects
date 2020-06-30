## What is a Docker Image?

A **Docker image** is an immutable (unchangeable) file that contains the source code,  libraries, dependencies, tools, and other files needed for an  application to run.

Due to their **read-only**  quality, these images are sometimes referred to as snapshots. They  represent an application and its virtual environment at a specific point in time. This consistency is one of the great features of Docker. It  allows developers to test and experiment software in stable, uniform  conditions.

Since images are, in a way, just **templates**, you cannot start or run them. What you can do is use that template as a base to build a container. A container is, ultimately, just a running  image. Once you create a container, it adds a writable layer on top of  the immutable image, meaning you can now modify it.

The image-based on which you create a container exists separately and cannot be altered. When you run a [containerized environment](https://phoenixnap.com/kb/how-to-containerize-applications), you essentially create a **read-write copy** of that filesystem (docker image) inside the container. This adds a **container layer** which allows modifications of the entire copy of the image.

![Brief explanation of Container Layer and Image layer](https://phoenixnap.com/kb/wp-content/uploads/2019/10/container-layers.png)

You can create an unlimited number of Docker images from one **image base**. Each time you change the initial state of an image and save the  existing state, you create a new template with an additional layer on  top of it.

Docker images can, therefore, consist of a **series of layers**, each differing but also originating from the previous one. Image layers represent read-only files to which a container layer is added once you  use it to start up a virtual environment.

## What is a Docker Container?

A **Docker container** is a virtualized run-time environment where users can isolate  applications from the underlying system. These containers are compact,  portable units in which you can start up an application quickly and  easily.

A valuable feature is the **standardization** of the computing environment running inside the container. Not only  does it ensure your application is working in identical circumstances,  but it also simplifies sharing with other teammates.

As containers are autonomous, they provide strong isolation, ensuring they do not  interrupt other running containers, as well as the server that supports  them. Docker claims that these units “provide the strongest isolation  capabilities in the industry”. Therefore, you won’t have to worry about  keeping your machine **secure** while developing an application.

Unlike virtual machines (VMs) where virtualization happens at the hardware  level, containers virtualize at the app layer. They can utilize one  machine, share its kernel, and virtualize the operating system to run  isolated processes. This makes containers extremely **lightweight**, allowing you to retain valuable resources.

![The difference in structure between containers and virtual machines](https://phoenixnap.com/kb/wp-content/uploads/2019/10/container-vs-virtual-machine.png)



# Networking

By default docker creates only one internal network

We can create our onw network with:

![image-20200616204638950](C:\Users\gmalarski\AppData\Roaming\Typora\typora-user-images\image-20200616204638950.png)

![image-20200616210117662](C:\Users\gmalarski\AppData\Roaming\Typora\typora-user-images\image-20200616210117662.png)

![image-20200616210411195](C:\Users\gmalarski\AppData\Roaming\Typora\typora-user-images\image-20200616210411195.png)

![image-20200616211927586](C:\Users\gmalarski\AppData\Roaming\Typora\typora-user-images\image-20200616211927586.png)

```dockerfile
docker run --network=wp-mysql-network -e DB_Host=mysql-db -e  DB_Password=db_pass123 -p 38080:8080 --name webapp --link  mysql-db:mysql-db -d kodekloud/simple-webapp-mysql
```