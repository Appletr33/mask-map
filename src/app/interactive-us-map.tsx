'use client'

import React, { useState, useEffect } from 'react'
import {
  ComposableMap,
  Geographies,
  Geography,
  ZoomableGroup,
} from 'react-simple-maps'
import { geoBounds } from 'd3-geo'
import * as topojson from 'topojson-client'

// USA map data
const geoUrl = 'https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json'
const countyUrl = 'https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json'

interface CountyEntry {
  fips_code: string;
  date: string;
  mask_order_code: string;
  masked_pub: string;
  citations: string;
}

interface StateMandateData {
  [key: string]: number;
}

let countyEntries: CountyEntry[];

const getRedShade = (value: number) => {
  const min = 0;
  const max = 1000;

  // Clamp the value between min and max
  const clampedValue = Math.min(Math.max(value, min), max);

  // Calculate the percentage (0 to 1)
  const percentage = (clampedValue - min) / (max - min);

  // Define lightness range: 80% (light) to 30% (dark)
  const lightness = 80 - 50 * percentage; // 80% to 30%

  return `hsl(0, 100%, ${lightness}%)`;
};

export default function Component() {
  const [tooltipContent, setTooltipContent] = useState('')
  const [selectedState, setSelectedState] = useState('')
  const [selectedStateId, setSelectedStateId] = useState(null)
  const [hoveredCounty, setHoveredCounty] = useState(null)
  const [selectedCounty, setSelectedCounty] = useState(null)
  const [center, setCenter] = useState([-97, 38]) // Initial center of the USA
  const [zoom, setZoom] = useState(1)
  const [countiesGeoJSON, setCountiesGeoJSON] = useState<JSON[] | null>(null)
  const [coloredCounties, setColoredCounties] = useState<{ [key: string]: number }>({})
  const [averageMandatesPerState, setAverageMandatesPerState] = useState<StateMandateData>({})

  // Fetch county GeoJSON data
  useEffect(() => {
    fetch(countyUrl)
      .then((res) => res.json())
      .then((data) => {
        // Convert TopoJSON to GeoJSON
        const counties = topojson.feature(data, data.objects.counties).features
        setCountiesGeoJSON(counties)
      })
      .catch((error) => {
        console.error('Error fetching county data:', error)
      })
  }, [])

  // Fetch average mandates per state when the page loads
  useEffect(() => {
    const fetchAverageMandates = async () => {
      try {
        const response = await fetch('/api/average-mandates-per-state');
        if (!response.ok) {
          throw new Error(`Error fetching data: ${response.statusText}`);
        }
        const data = await response.json();
        // Format the data into { stateName: averageMandateCount }
        const formattedData = data.average_mandates_per_state.reduce((acc: StateMandateData, item: { state: string; average_mandate_count: number }) => {
          const stateName = item.state;
          acc[stateName] = item.average_mandate_count;
          return acc;
        }, {});
        setAverageMandatesPerState(formattedData);
      } catch (error) {
        console.error('Error fetching average mandates per state:', error);
      }
    };
    fetchAverageMandates();
  }, []);

  // Fetch colored counties data when a state is selected
  const fetchCountyData = async (state: string) => {
    setColoredCounties({});
    try {
      const response = await fetch(`/api/colored-counties?state=${encodeURIComponent(state)}`);
      if (!response.ok) {
        throw new Error(`Error fetching data: ${response.statusText}`);
      }

      const data = await response.json();
      console.log(data);

      const formattedData = data["mandates"].reduce((acc: { [key: string]: number }, item: { countyName: string; mandateCount: number }) => {
        const countyName = item.countyName.toLowerCase(); // Convert county name to lowercase for consistency
        acc[countyName] = item.mandateCount; // Set the mandate count as the value for each county
        return acc;
      }, {});

      // Update the state with the formatted data
      setColoredCounties(formattedData);
    } catch (error) {
      console.error('Error:', error);
    }
  };

  // Fetch county data when a state is selected
  useEffect(() => {
    if (selectedState) {
      fetchCountyData(selectedState);
    }
  }, [selectedState]);

  // Handle state click to zoom in and fetch county data
  const handleStateClick = (geo: any) => {
    const stateName = geo.properties.name;
    const stateId = geo.id;

    let bounds;

    if (stateName === "Alaska") {
      bounds = [
        [-179.148909, 51.214183],
        [-129.9795, 71.365162]
      ];
    } else {
      bounds = geoBounds(geo);
    }
    const centerX = (bounds[0][0] + bounds[1][0]) / 2;
    const centerY = (bounds[0][1] + bounds[1][1]) / 2;

    setCenter([centerX, centerY]);

    const stateWidth = bounds[1][0] - bounds[0][0];
    const stateHeight = bounds[1][1] - bounds[0][1];

    const screenWidth = window.innerWidth;
    const screenHeight = window.innerHeight;

    const screenAspectRatio = screenWidth / screenHeight;
    const stateAspectRatio = stateWidth / stateHeight;

    let zoomFactor;

    if (stateAspectRatio > screenAspectRatio) {
      zoomFactor = screenWidth / stateWidth;
    } else {
      zoomFactor = screenHeight / stateHeight;
    }

    zoomFactor *= 0.02;
    if (stateName === "Alaska") {
      zoomFactor *= 4;
    }

    setZoom(zoomFactor);

    setSelectedState(stateName);
    setSelectedStateId(stateId);
    setSelectedCounty(null);
    setTooltipContent(stateName);
  };

  // Handle county hover
  const handleCountyHover = (geo: any) => {
    const countyName = geo.properties.name;
    setHoveredCounty(geo.id);
    setTooltipContent(`${countyName} County`);
  };

  const handleCountyLeave = () => {
    setHoveredCounty(null);
    setTooltipContent(selectedState);
  };

  // Handle county click
  const handleCountyClick = (geo: any) => {
    setSelectedCounty(geo.id);
  };

  // Reset view to the initial state
  const handleResetClick = () => {
    setSelectedState('');
    setSelectedStateId(null);
    setCenter([-97, 38]);
    setZoom(1);
    setTooltipContent('');
    setHoveredCounty(null);
    setSelectedCounty(null);
    setColoredCounties({});
  };

  if (!countiesGeoJSON) {
    return <div>Loading...</div>;
  }

  return (
    <div className="relative w-full h-screen bg-gray-100">
      <ComposableMap projection="geoAlbersUsa">
        <ZoomableGroup center={center as [number, number]} zoom={zoom}>
          {/* Render States */}
          <Geographies geography={geoUrl}>
            {({ geographies }) =>
              geographies.map((geo) => {
                const stateName = geo.properties.name;
                const isSelected = selectedState === stateName;

                // Use average mandate data to color unselected states
                const averageMandate = averageMandatesPerState[stateName];
                const fillColor = isSelected
                  ? '#ADD8E6' // Highlight selected state
                  : averageMandate !== undefined
                    ? getRedShade(averageMandate) // Color based on average mandate
                    : '#DDD'; // Default color

                return (
                  <Geography
                    key={geo.rsmKey}
                    geography={geo}
                    fill={fillColor}
                    stroke="#FFF"
                    strokeWidth={0.5}
                    onClick={() => handleStateClick(geo)}
                    onMouseEnter={() => {
                      const { name } = geo.properties;
                      setTooltipContent(`${name}`);
                    }}
                    onMouseLeave={() => {
                      setTooltipContent(selectedState || '');
                    }}
                    style={{
                      default: { outline: 'none' },
                      hover: { outline: 'none', fill: '#ADD8E6' },
                      pressed: { outline: 'none', fill: '#E42' },
                    }}
                  />
                );
              })
            }
          </Geographies>

          {/* Render Counties if a state is selected */}
          {selectedStateId && (
            <Geographies geography={countyUrl}>
              {({ geographies }) =>
                geographies
                  .filter((geo) => geo.id.startsWith(selectedStateId))
                  .map((geo) => (
                    <Geography
                      key={geo.rsmKey}
                      geography={geo}
                      fill={
                        selectedCounty === geo.id
                          ? '#0091C0' // Selected county color
                          : typeof coloredCounties[geo.properties.name.toLowerCase()] === 'number'
                            ? getRedShade(coloredCounties[geo.properties.name.toLowerCase()]) // Dynamic red shade based on value
                            : hoveredCounty === geo.id
                              ? '#ADD8E6' // Hovered county color
                              : '#D3D3D3' // Default color
                      }
                      stroke="#FFF"
                      strokeWidth={0.3}
                      onMouseEnter={() => handleCountyHover(geo)}
                      onMouseLeave={handleCountyLeave}
                      onClick={() => handleCountyClick(geo)}
                      style={{
                        default: { outline: 'none' },
                        hover: { outline: 'none' },
                        pressed: { outline: 'none' },
                      }}
                    />
                  ))
              }
            </Geographies>
          )}
        </ZoomableGroup>
      </ComposableMap>

      {/* Tooltip for hovered county/state */}
      {tooltipContent && (
        <div
          style={{
            position: 'absolute',
            top: 10,
            left: 10,
            backgroundColor: 'white',
            color: 'black',
            padding: '5px',
            borderRadius: '5px',
            boxShadow: '0 0 10px rgba(0, 0, 0, 0.1)',
            fontSize: '12px',
            lineHeight: '1.5',
            maxWidth: '300px',
            whiteSpace: 'pre-wrap',
          }}
        >
          {tooltipContent}
        </div>
      )}

      {/* Toolbox for selected county */}
      {countyEntries && (
        <div
          style={{
            position: 'absolute',
            bottom: 10,
            left: 10,
            backgroundColor: 'white',
            color: 'black',
            padding: '10px',
            borderRadius: '5px',
            boxShadow: '0 0 10px rgba(0, 0, 0, 0.1)',
            fontSize: '14px',
            lineHeight: '1.5',
            maxWidth: '300px',
          }}
        >
          <h3 className="font-bold mb-2">{countyEntries[0].mask_order_code} County</h3>
          <p>Mask Order Code: {countyEntries[0].masked_pub}</p>
        </div>
      )}

      {/* Reset Button */}
      {selectedStateId && (
        <button
          onClick={handleResetClick}
          className="absolute top-4 right-4 bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
        >
          Reset View
        </button>
      )}
    </div>
  );
}
