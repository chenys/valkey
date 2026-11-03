/* Run low-level listpack tests without enabling all REDIS_TEST server code. */
int listpackTest(int argc, char **argv);

int main(int argc, char **argv) {
    return listpackTest(argc, argv);
}
