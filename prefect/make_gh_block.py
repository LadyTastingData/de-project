from prefect.filesystems import GitHub

# alternative to creating GitHub block in the UI

gh_block = GitHub(
    name="de-project-ghblock", repository="https://github.com/topahande/de-project"
)

gh_block.save("de-project-ghblock", overwrite=True)
