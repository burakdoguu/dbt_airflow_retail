with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('product_category_map') }}

),

renamed as (

    select
        productid,
        categoryid

    from source

)

select * from renamed
