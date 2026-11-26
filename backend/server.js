import express from "express";
import cors from "cors";
import dotenv from "dotenv";


const express = require("express");
const cors = require("cors");
require("dotenv").config();

const app = express();
const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");



app.use(cors());
app.use(express.json());

// Home route
app.get("/", (req, res) => {
    res.json({
        message: "Smart Traffic Management Backend is running"
    });
});

// Health check
app.get("/api/health", (req, res) => {
    res.json({
        status: "success",
        message: "Backend connected successfully"
    });
});

// Sample traffic data
app.get("/api/traffic", (req, res) => {
    res.json([
        {
            location_id: "DELHI_001",
            location: "Connaught Place",
            traffic_volume: 1200,
            avg_vehicle_speed: 12,
            congestion_status: "HIGH CONGESTION"
        },
        {
            location_id: "DELHI_002",
            location: "India Gate",
            traffic_volume: 500,
            avg_vehicle_speed: 45,
            congestion_status: "NORMAL"
        },
        {
            location_id: "DELHI_003",
            location: "Karol Bagh",
            traffic_volume: 950,
            avg_vehicle_speed: 18,
            congestion_status: "HIGH CONGESTION"
        }
    ]);
});

const PORT = process.env.PORT || 5000;

app.listen(PORT, "0.0.0.0", () => {
    console.log(`Backend running on port ${PORT}`);
});
