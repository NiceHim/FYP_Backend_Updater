import axios from "axios";

const base = axios.create({
    baseURL: process.env.FASTAPI_BACKEND,
    timeout: 5000,
});

axios.interceptors.response.use(function (response) {
    return response;
}, function (error) {
    return Promise.reject(error);
});

export default base;